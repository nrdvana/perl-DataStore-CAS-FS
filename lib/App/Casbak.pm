package App::Casbak;
use strict;
use warnings;
use File::CAS;
use File::Spec;
use Carp;
use Try::Tiny;
use YAML::XS ();

=head1 NAME

Casbak - backup utility built around File::CAS storage library

=head1 LOGGING

Casbak defines class methods for logging purposes.
They are called as

  App::Casbak::Error(@things)

where @things can contain objects with auto-stringification.
*However* in the methods Debug() and Trace() objects will be dumped
with Data::Dumper regardless of whether they supply stringification.

No stringification occurs at all unless the log level has enabled
the function.

Functions are Error, Warn, Note, Into, Debug, Trace, and the default
level is to display Note and above.

Call App::Casbak->SetLogLevel($integer) to set the log level.

(at some point in the future, these will be directable to custom
 user defined logging modules, and SetLogLevel will be ignored)

=cut

our $LogLevel= 0;
sub SetLogLevel { (undef, $LogLevel)= @_; }
sub Error { return unless $LogLevel > -3; print STDERR "Error: ".join(" ", @_)."\n"; }
sub Warn  { return unless $LogLevel > -2; print STDERR "Warning: ".join(" ", @_)."\n"; }
sub Note  { return unless $LogLevel > -1; print STDERR "Notice: ".join(" ", @_)."\n"; }
sub Info  { return unless $LogLevel >= 1; print STDERR "Info: ".join(" ", @_)."\n"; }
sub Debug { return unless $LogLevel >= 2; print STDERR "Debug: ".Data::Dumper::Dumper(@_); }
sub Trace { return unless $LogLevel >= 3; print STDERR "Trace: ".Data::Dumper::Dumper(@_); }

our $VERSION= "0.0100";

sub VersionParts {
	return (int($VERSION), (int($VERSION*100)%100), (int($VERSION*10000)%100));
}

sub VersionMessage {
	"casbak backup utility, Copyright 2012 Michael Conrad\n"
	."App::Casbak version: ".join('.',VersionParts())."\n"
	."File::CAS version: ".join('.',File::CAS::VersionParts())."\n";
}

*_requireClass= *File::CAS::_requireClass;

sub backupDir { $_[0]{backupDir} }
sub cfgFile   { File::Spec->catfile($_[0]->backupDir, 'casbak.conf.yml') }
sub logFile   { File::Spec->catfile($_[0]->backupDir, 'casbak.log') }
sub rootsFile { File::Spec->catfile($_[0]->backupDir, 'casbak.snapshots') }
sub storeDefaultPath { File::Spec->catdir($_[0]->backupDir, 'store') }

sub snapshots {
	my $self= shift;
	$self->{snapshots} ||= do {
		open my $fh, "<", $self->rootsFile
			or die "Failed to read '".$self->rootsFile."': $!\n";
		my @entries;
		while (<$fh>) {
			my @fields= split /[\t\n]/;
			scalar(@fields) >= 2
				or die "Invalid entry in '".$self->rootsFile."': \"$_\"\n";
			push @entries, \@fields;
		}
		\@entries;
	};
}

sub getSnapshot {
	my ($self, $targetDate)= @_;
	my $array= $self->snapshots;
	
	return undef unless @$array;
	
	# if targetEpoch is undef, return the latest snapshot
	return $array->[-1] unless defined $targetDate;
	$targetDate= $self->canonicalDate($targetDate);
	
	my ($min, $max, $mid)= (-1, $#$array);
	while ($min < $max) {
		$mid= ($min+$max+1)>>1;
		if ($targetDate ge $array->[$mid][0]) {
			$min= $mid;
		} else {
			$max= $mid-1;
		}
	}
	return $max < 0? undef : $array->[$min];
}

sub getSnapshotOrDie {
	my ($self, $date)= @_;
	$self->getSnapshot($date)
		or die defined $date? "No snapshot existed on date $date\n" : "No snapshots recorded yet\n";
}
	
sub saveSnapshot {
	my ($self, $date, $hash)= @_;
	push @{$self->snapshots}, [ $self->canonicalDate($date), $hash ];
	my $tmpFile= $self->rootsFile.'.tmp';
	try {
		open my $fh, ">", $tmpFile
			or die "Cannot write to '$tmpFile': $!\n";
		for my $snap (@{$self->snapshots}) {
			print $fh join("\t", @$snap)."\n";
		}
		close $fh
			or die "Cannot save '$tmpFile': $!\n";
		rename $tmpFile, $self->rootsFile
			or die "Cannot replace '".$self->rootsFile."': $!\n";
	}
	catch {
		die "$_\nThe entry that would have been written is: '$date $hash'\n";
	};
	1;
}

sub getConfig {
	my $self= shift;
	return {
		CLASS => ref $self,
		VERSION => $VERSION,
		cas => $self->cas->getConfig,
	};
}

sub cas { $_[0]{cas} }

sub new {
	my $class= shift;
	my %p;
	if (scalar(@_) eq 1) {
		if (ref $_[0]) {
			%p= %{$_[0]};
		} else {
			-d $_[0] or die "No such backup directory: $_[0]\n";
			%p= ( backupDir => $_[0] );
		}
	} else {
		%p= @_;
	}
	
	$p{backupDir}= '.' unless defined $p{backupDir};
	if (!$p{cas}) {
		require YAML;
		my $cfgFile= File::Spec->catfile($p{backupDir}, 'casbak.conf.yml');
		-f $cfgFile or die "Missing config file '$cfgFile'\n";
		-r $cfgFile or die "Permission denied for '$cfgFile'\n";
		my $cfg= YAML::LoadFile($cfgFile)
			or die "Failed to load config file '$cfgFile'\n";
		%p= (%$cfg, %p);
	}
	
	# coersion from hash to object
	if (ref $p{cas} eq 'HASH') {
		my %cp= %{$p{cas}};
		my $cclass= (delete $cp{CLASS}) || 'File::CAS';
		my $cclass_ver= delete $cp{VERSION};
		DynLoad($cclass, $cclass_ver);
		$cclass->isa('File::CAS')
			or die "'$cclass' is not a valid CAS class\n";
		
		if (ref $cp{store} eq 'HASH') {
			DynLoad($cp{store}{CLASS}, delete $cp{store}{VERSION});
			
			my %validParams= map { $_ => 1 } $cp{store}{CLASS}->_ctor_params;
			# We don't store the 'pathBase' in the configuration, so that path stays relative to backupDir
			$cp{store}{pathBase}= $p{backupDir} if $validParams{pathBase};
		}
		
		if (ref $cp{scanner} eq 'HASH') {
			DynLoad($cp{scanner}{CLASS}, delete $cp{scanner}{VERSION});
		}
		
		$p{cas}= $cclass->new(\%cp);
	}
	
	$class->_ctor(\%p);
}

sub _ctor {
	my ($class, $params)= @_;
	($params->{VERSION}||0) <= $VERSION
		or die "App::Casbak v$VERSION cannot support backup created with version $params->{VERSION} !\nAborting\n";
	
	defined $params->{backupDir} or croak "Missing required param 'backupDir'";
	defined $params->{cas} and $params->{cas}->isa('File::CAS') or croak "Missing/invalid required param 'cas'";

	bless $params, $class;
}

sub dateParser {
	my $self= shift;
	require DateTime;
	require Date::Format::Natural;
	$self->{dateParser} ||= DateTime::Format::Natural->new;
}

sub canonicalDate {
	my ($self, $date)= @_;
	if (!ref $date) {
		# is it already in a valid format?
		if ($date =~ /^\d\d\d\d-\d\d(-\d\d(T\d\d:\d\d(:\d\d)?)?)?Z$/) {
			# supply missing parts of date, after clipping off 'Z'
			return substr($date, 0, -1) . substr('0000-00-00T00:00:00Z', length($date)-1);
		}
		# is it a relative date?
		if ($date =~ /^(\d+)([DWMY])$/) {
			require DateTime;
			my %field= ( D => 'days', W => 'weeks', M => 'months', Y => 'years' );
			$date= DateTime->now->add( $field{$2} => $1 );
		}
		# else we have to parse it
		else {
			my $dt= $self->dateParser->extract_datetime($date);
			$self->dateParser->success
				or die "Invalid date: '$date'\n";
			if ($dt->time_zone eq 'floating') {
				$dt->set_time_zone('local');
			}
			$dt->set_time_zone('UTC');
			return $dt->ymd.'T'.$dt->hms.'Z';
		}
	} else {
		# or if its already a DateTime object, we just have to check the timezone
		if ($date->time_zone ne 'UTC') {
			$date= $date->clone();
			$date->set_time_zone('UTC');
		}
		return $date->ymd.'T'.$date->hms.'Z';
	}
}

sub init {
	my ($class, $params)= @_;
	Trace('Casbak->init(): ', $params);
	my $dir= $params->{backupDir} || '.';
	my @entries= grep { $_ ne '.' && $_ ne '..' } <$dir/*>;
	scalar(@entries)
		and die "Backups may only be initialized in an empty directory\n";
	
	$params->{cas}{store} ||= { CLASS => 'File::CAS::Store::Simple' };
	$params->{cas}{store}= { CLASS => $params->{store} }
		unless ref $params->{cas}{store};
	$params->{cas}{store}{CLASS}->can('new')
		or require File::Spec->catfile(split('::',$params->{cas}{store}{CLASS})).'.pm';
	
	my %validParams= map { $_ => 1 } $params->{cas}{store}{CLASS}->_ctor_params;
	$params->{cas}{store}{path}= 'store'
		if (!defined $params->{cas}{store}{path} and $validParams{path});
	$params->{cas}{store}{create}= 1
		if (!defined $params->{cas}{store}{create} and $validParams{create});
	
	my $self= $class->new($params);
	
	# success? then save out the parameters
	my $fd;
	require YAML;
	my $cfg= YAML::Dump($self->getConfig);
	open($fd, ">", $self->cfgFile) && (print $fd $cfg) && close($fd)
		or die "Error writing configuration file '".$self->cfgFile."': $!\n";
	open($fd, ">", $self->logFile) && close($fd)
		or die "Error writing log file: $!\n";
	open($fd, ">", $self->rootsFile) && close($fd)
		or die "Error writing snapshots file: $!\n";
}

sub importTree {
	my ($self, %p)= @_;
	Trace('Casbak->importTree(): ', \%p);
	
	my ($srcPath, $dstPath, $rootEnt)= ($p{real}, $p{virt}, $p{root});

	# The Root Dir::Entry defaults to the latest snapshot
	$rootEnt= $self->getSnapshot()
		unless defined $rootEnt;
	
	# If we're starting from *nothing*, we fake the root Dir::Entry by
	#   supplying the known hash of the canonical "Empty Directory"
	$rootEnt= $self->cas->getEmptyDirHash()
		unless defined $rootEnt;
	
	# Did they give is a proper DirEnt, or just a hash?
	if (!ref $rootEnt) {
		# They gave us a hash.  Convert to Dir::Entry.
		$rootEnt= File::CAS::Dir::Entry->new( name => '', type => 'dir', hash => $rootEnt );
	}
	else {
		ref($rootEnt)->isa('File::CAS::Dir::Entry')
			or croak "Invalid 'root': must be File::CAS::Dir::Entry or digest string";
		$rootEnt->type eq 'dir'
			or croak "Root directory entry must describe a directory.";
	}
	
	# Now get an array of Dir::Entry describing the entire destination path.
	# Any missing directories will create generic/empty Dir::Entry objects.
	my $err;
	my $resolvedDest= $self->cas->resolvePathPartial($rootEnt, $dstPath, \$err);
	
	# We always allow the final path element to be created/overwritten, but we only
	# allow inbetween directories to be created if the user requested that feature.
	if (@$resolvedDest > 1 and !defined $resolvedDest->[-2]->hash) {
		$p{create_deep}
			or croak "Destination path does not exist in backup: '$dstPath' ($err)";
	}

	# Now inspect the source entry in the real filesystem
	my $srcEnt= $self->cas->scanner->scanDirEnt($srcPath)
		or die "Cannot stat '$srcPath'\n";
	# Its probably a dir, but we also allow importing single files.
	if ($srcEnt->{type} eq 'dir') {
		my $hintDir;
		if (defined $resolvedDest->[-1]) {
			croak "Attempt to overwrite file with directory"
				if (defined $resolvedDest->[-1]->type and $resolvedDest->[-1]->type ne 'dir');
			$hintDir= $self->cas->getDir($resolvedDest->[-1]->hash)
				if (defined $resolvedDest->[-1]->hash);
		}
		my $hash= $self->cas->putDir($srcPath, $hintDir);
		
		# When building the new dir entry, keep all source attrs except name,
		#   but use destination entry attrs as defaults for attrs not set in $srcEnt
		# Example:
		#  $srcEnt = { name => 'foo', create_ts => 12345 };
		#  $dstEnt = { name => 'bar', create_ts => 11111, unix_uid => 1002 };
		#  $result = { name => 'bar', create_ts => 12345, unix_uid => 1002 };
		my %attrs= %{$srcEnt->asHash};
		delete $attrs{name};
		%attrs= %{$dstEnt->asHash}, %attrs;
		$resolvedDest->[-1]= File::CAS::Dir::Entry->new(%attrs);
	}
	else {
		# We do not allow the root entry to be anything other than a directory.
		(@$resolvedDest > 1)
			or croak "Cannot store non-directory (type = ".$srcEnt->type.") as virtual root: '$srcPath'\n";
		my $hash= $self->cas->putFile($srcPath);
		$resolvedDest->[-1]= File::CAS::Dir::Entry->new( %{$srcEnt->asHash}, hash => $hash );
	}
	
	# The final Dir::Entry in the list $resolvedDest has been modified.
	# If it was not the root, then we need to walk up the tree modifying each directory.
	while (@$resolvedDest > 1) {
		my $newEnt= pop @$resolvedDest;
		my $dirEnt= $resolvedDest->[-1];
		my $dir= $self->cas->getDir($dirEnt->hash);
		$dir= $self->cas->mergeDir($dir, [ $newEnt ] );
		my $hash= $self->cas->putDir($dir);
		$resolvedDest->[-1]= File::CAS::Dir::Entry->new( %{$dirEnt->asHash}, hash => $hash );
	}
	
	# Return the new root Dir::Entry (caller will likely save this as a snapshot)
	return $resolvedDest[0];
}


1;
