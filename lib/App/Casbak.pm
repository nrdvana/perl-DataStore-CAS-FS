package App::Casbak;
use strict;
use warnings;
use File::CAS;
use File::Spec;
use Carp;
use Try::Tiny;

require App::Casbak::ImportFile;

our $LogLevel= 0;
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

sub CmdlineOptions {
	my ($paramHash)= shift;
	my $callerPkg= caller;
	
	$paramHash->{verbose}= 0;
	return (
		'version|V'      => sub { print VersionMessage(); exit 0; },
		'help|?'         => sub { require Pod::Usage; Pod::Usage::pod2usage(-verbose => 2); exit 1; },
		'verbose|v'      => sub { ++$LogLevel; },
		'quiet|q'        => sub { --$LogLevel; },
		'casbak-dir|D=s' => \$paramHash->{backupDir},
	);
}

sub DynLoad {
	my ($module, $version)= @_;
	Trace("Loading module '$module' (version $version)");
	($module =~ /^[A-Za-z0-9:]+$/)
		or carp "Invalid perl package name: '$module'\n";
	$version= '' unless defined $version;
	($version =~ /^[^ ]*$/)
		or carp "Invalid version string: '$version'\n";
	$module->can('new') or do {
		try {
			eval "use $module $version";
		}
		catch {
			carp "$_";
		}
	}
}

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
	
sub addSnapshot {
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

1;
