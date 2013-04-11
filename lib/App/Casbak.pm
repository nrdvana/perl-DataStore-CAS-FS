package App::Casbak;
use Moo;
use Carp;
use Try::Tiny;
use JSON ();
use File::Spec;
use DataStore::CAS::FS;

=head1 NAME

Casbak - backup utility built around DataStore::CAS::FS library

=head1 SYNOPSIS

  # Create a new backup
  my $cb= App::Casbak->init(
    casbak_dir => '~/backups,
    cas => 'DataStore::CAS::Simple',
    ...
  );
  
  # Store some files
  my $cb= App::Casbak->new( casbak_dir => '~/backups' );
  my $snap= $cb->get_snapshot(); # loads latest snapshot
  my $fs= DataStore::CAS::FS->new( root => $snap->root_entry );
  $cb->import_tree( fs => $fs, real => "~/foo", virt => '/' );
  $cb->save_snapshot( fs => $fs );
  
  # Extract some files
  my $snap= $cb->get_snapshot( '2013-04-01' );
  my $fs= DataStore::CAS::FS->new( root => $snap->root_entry );
  $cb->export_tree( fs => $fs, virt => '/', real => '/tmp/restore' );

=head1 DESCRIPTION

Casbak is a utility that creates backups of filesystems by storing them in
content-addressable-storage.  It is much like Git, but preserves as much
filesystem metadata as you want, and has the ability to selectively purge
old backups. (purging capability actually depends on the CAS backend; at the
moment it is designed but not implemented.  Coming soon.)

Casbak is a practical wrapper around the library DataStore::CAS::FS, which is
a virtual filesystem using DataStore::CAS as a storage backend.  If you want a
CAS implementation with fancy features like file-chunking and etc, all you
need to do is pick your favorite DataStore::CAS module, or write one yourself.
DataStore::CAS has an extremely simple API.

The Casbak utility is composed of this module (App::Casbak) and a number of
command classes under App::Casbak::Cmd namespace which implement the command
line interface.  You can easily extend Casbak by writing new perl modules in
the App::Casbak::Cmd:: namespace and adding them to your perl module path.

=head1 LOGGING

Casbak defines class methods for logging purposes.
They are called as

  App::Casbak::Error(@things)

where @things can contain objects with auto-stringification.  *However* in
the methods Debug() and Trace(), objects will be dumped with Data::Dumper
(or Data::Printer) regardless of whether they supply stringification.

No stringification occurs at all unless the log level has enabled
the function.

Functions are Error, Warn, Note, Into, Debug, Trace, and the default
level is to display Note and above.

Call App::Casbak->SetLogLevel($integer) to set the log level.

(at some point in the future, these will be directable to custom
 user defined logging modules, and SetLogLevel will be ignored)

=cut

our $LogLevel= 0;
sub SetLogLevel { $LogLevel= $_[-1]; }
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
	."DataStore::CAS::FS version: ".join('.',DataStore::CAS::FS::VersionParts())."\n";
}

our $_config_filename=    'casbak.conf.json';
our $_log_filename=       'casbak.log';
our $_snapshots_filename= 'casbak.snapshots.tsv';

=head1 ATTRIBUTES


=head2 date_parser

Returns a cached instance of the parser we use in the event that
we need to parse a date string.  This is lazy-built.

=cut

has backup_dir         => ( is => 'ro', required => 1, default => sub { '.' } );
has cas                => ( is => 'ro', coerce => \&_coerce_cas );
has scanner            => ( is => 'ro', coerce => \&_coerce_scanner );
has extractor          => ( is => 'ro', coerce => \&_coerce_extractor );
has store_default_path => ( is => 'ro', default => sub { 'store' } );
has snapshot_index     => ( is => 'rw', lazy_build => 1 );
has date_parser        => ( is => 'rw', lazy_build => 1 );

=head1 METHODS

=cut

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
	
sub BUILD {
	my $self= shift;
	
	if (! ref $self->cas) {
		
	}
	

	$p{backup_dir}= '.' unless defined $p{backup_dir};
	# coersion from hash to object
	if (ref $p{cas} eq 'HASH') {
		my %cp= %{$p{cas}}; # make a copy; we're going to modify it

		# Determine whether to use File::CAS or a subclass.
		my $cclass= (delete $cp{CLASS}) || 'File::CAS';
		my $cclass_ver= delete $cp{VERSION};
		_requireClass($cclass, $cclass_ver);
		$cclass->isa('File::CAS')
			or die "'$cclass' is not a valid CAS class\n";

		# If the store is a configuration and not an object yet,
		# we muck around with its parameters a bit.
		if (ref $cp{store} eq 'HASH') {
			$cp{store}= { %{$cp{store}} }; # clone it
			
			# If the constructor has a 'path' parameter, we convert it from relative to
			# absolute, because we want it to be relative to the ->backupDir and not
			# to the current directory.
			if (defined $cp{store}{path}) {
				$cp{store}{path}= File::Spec->rel2abs($cp{store}{path}, $p{backupDir})
					unless File::Spec->file_name_is_absolute($cp{store}{path});
			}
		}
		elsif (ref $cp{store} and ref($cp{store})->can('path')) {
			Warn("using a CAS storage engine with a path relative to the current dir (not the casbak dir)")
				unless File::Spec->file_name_is_absolute($cp{store}{path})
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

sub _build_date_parser {
	require DateTime;
	require Date::Format::Natural;
	DateTime::Format::Natural->new;
}

=head2 canonicalDate( $date_thing )

Takes a date in a variety of formats and returns a string
in "YYYY-MM-DDTHH:MM:SSZ" format.

  my $date= $self->canonicalDate( "2000-01-01Z" );
  my $date= $self->canonicalDate( "25D" );
  my $date= $self->canonicalDate( time() );
  my $date= $self->canonicalDate( DateTime->new() );

Allowed formats include canonical date strings or recognizable
fractions of one, unix 'time()' integers, DateTime objects,
or relative notations like "25D" (25 days ago), "2W" (2 weeks ago),
"2Y" (2 years ago), or "3M" (3 months ago).

=cut

sub canonicalDate {
	my ($self, $date)= @_;
	if (!ref $date) {
		# is it already in a valid format?
		if ($date =~ /^\d\d\d\d-\d\d(-\d\d(T\d\d:\d\d(:\d\d)?)?)?Z$/) {
			# supply missing parts of date, after clipping off 'Z'
			# We can return here without needing to create a DateTime object.
			return substr($date, 0, -1) . substr('0000-00-00T00:00:00Z', length($date)-1);
		}

		# is it a single integer? (unix epoch time)
		if ($date =~ /^\d+$/) {
			my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)= gmtime($date);
			# We can return here without needing to create a DateTime object.
			return sprintf('%4d-%2d-%2dT%2d:%2d:%2dZ', $year+1900, $mon, $mday, $hour, $min, $sec);
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
				or croak "Invalid date: '$date'\n";
			$date= $dt;
		}
	}
	else {
		croak "Expected DateTime object but got ".ref($date)
			unless $date->isa('DateTime');
		$date= $date->clone();
	}

	# Convert timezone to UTC.  Floating is treated as local time.
	$date->set_time_zone('local')
		if ($date->time_zone eq 'floating');
	$date->set_time_zone('UTC')
		if ($date->time_zone ne 'UTC');

	return $date->ymd.'T'.$date->hms.'Z';
}

sub _build_snapshot_index {
	my $self= shift;
	$self->_read_snapshot_index($self->backup_dir);
}

sub getSnapshot {
	my ($self, $targetDate)= @_;
	my $array= $self->snapshot_index;
	
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
	return undef if $max < 0;
	
	my $hash= $array->[$min][1];
	my $snap= $self->cas->getDir($hash);
	$snap;
}

sub getSnapshotOrDie {
	my ($self, $date)= @_;
	$self->getSnapshot($date)
		or die defined $date? "No snapshot existed on date $date\n" : "No snapshots recorded yet\n";
}
	
sub saveSnapshot {
	my ($self, $entries, $metadata)= @_;
	
	ref $entries eq 'ARRAY'
		and scalar(@$entries) == 1
		and $entries->[0]->name eq ''
		or croak "Expected array of one entry named ''";

	# Default the timestamp to 'now'
	$metadata->{timestamp}= time
		unless defined $metadata->{timestamp};
	
	# Convert to standard date format
	$metadata->{timestamp}= $self->canonicalDate($metadata->{timestamp});
	
	my $array= $self->snapshot_index;
	# Timestamps must be in cronological order
	# (we could insert-sort here, but people should only ever be adding "new" snapshots...)
	!scalar(@$array)
		or $metadata->{timestamp} ge $array->[-1][0]
		or croak "New timestamp '$metadata->{timestamp}' must be later than last recorded timestamp '$array->[-1][0]'";
	
	# Serialize the new snapshot and store it in the CAS
	my $encoded= App::Casbak::Snapshot->SerializeEntries($entries, $metadata);
	my $hash= $self->cas->put($encoded);
	
	# Append the new snapshot to the end of the snapshot index
	push @$array, [ $metadata->{timestamp}, $hash, $metadata->{comment} || '' ];
	# and write the new index file
	try {
		$self->_write_snapshot_index($self->backup_dir, $array);
	}
	catch {
		die "$_\nThe entry that would have been written is: '$metadata->{timestamp} $hash'\n";
	};
	1;
}

sub init {
	my ($class, $params)= @_;
	Trace('Casbak->init(): ', $params);

	# Default to current dir
	my $dir= $params->{backupDir} || '.';
	
	# Directory must exist and be empty
	my @entries= grep { $_ ne '.' && $_ ne '..' } <$dir/*>;
	-d $dir and !scalar(@entries)
		or croak "Backups may only be initialized in an empty directory\n";

	# The default store is Store::Simple
	$params->{cas}{store} ||= { CLASS => 'File::CAS::Store::Simple' };

	# Auto-coerce class names into a hash with CLASS key.
	$params->{cas}{store}= { CLASS => $params->{store} }
		unless ref $params->{cas}{store};

	# Make sure module is loaded, so we can inspect its constructor params
	$params->{cas}{store}{CLASS}->can('new')
		or require_module($params->{cas}{store}{CLASS});

	my %validParams= map { $_ => 1 } $params->{cas}{store}{CLASS}->_ctor_params;

	# If the store class supports 'path', we supply $backupDir/store as the default.
	$params->{cas}{store}{path}= 'store'
		if (!defined $params->{cas}{store}{path} and $validParams{path});

	# If the store class supports 'create', we request it.
	$params->{cas}{store}{create}= 1
		if (!defined $params->{cas}{store}{create} and $validParams{create});

	# Initialize snapshotIndex to prevent it from getting loaded from a file that doesn't exist yet.
	$params->{snapshotIndex}= [];

	my $self= $class->new($params);

	# success? then save out the parameters
	$class->_initialize_backup(backup_dir => $dir, cfg => 
	my $cfg= $self->getConfig;
	my $json= JSON->new->utf8->pretty->canonical->encode($cfg);
	my $fd;
	open($fd, ">", $self->cfgFile) && (print $fd $json) && close($fd)
		or croak "Error writing configuration file '".$self->cfgFile."': $!\n";
	open($fd, ">", $self->logFile) && close($fd)
		or croak "Error writing log file: $!\n";
	$self->_writeSnapIndex;
	$self;
}

sub importTree {
	my ($self, %p)= @_;
	Trace('Casbak->importTree(): ', \%p);
	
	my ($srcPath, $dstPath, $rootEnt)= ($p{real}, $p{virt}, $p{root});

	# The Root Dir::Entry defaults to the latest snapshot
	unless (defined $rootEnt) {
		my $snap= $self->getSnapshot();
		$rootEnt= $snap->rootEntry
			if defined $snap;
	}
	
	# If we're starting from *nothing*, we fake the root Dir::Entry by
	#   supplying the known hash of the canonical "Empty Directory"
	$rootEnt= $self->cas->getEmptyDirHash()
		unless defined $rootEnt;
	
	# Did they give us a proper DirEnt, or just a hash?
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
	croak "Can't resolve destination directory: $err"
		if defined $err;
	
	# We always allow the final path element to be created/overwritten, but we only
	# allow inbetween directories to be created if the user requested that feature.
	if (@$resolvedDest > 1 and !defined $resolvedDest->[-2]->hash) {
		$p{create_deep}
			or croak "Destination path does not exist in backup: '$dstPath' ($err)";
	}

	# Now inspect the source entry in the real filesystem
	my $srcEnt= $self->cas->scanner->scanDirEnt($srcPath)
		or croak "Cannot stat '$srcPath'";
	# It is probably a dir, but we also allow importing single files.
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
		%attrs= ( %{$resolvedDest->[-1]->asHash}, %attrs );
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
	return $resolvedDest->[0];
}

XXX
	# coercion of store parameters to Store object
	if (!ref $p{store}) {
		$p{store}= { CLASS => $p{store} };
	}
	if (ref $p{store} eq 'HASH') {
		$p{store}= { %{$p{store}} }; # clone before we make changes
		my $class= delete $p{store}{CLASS} || 'DataStore::CAS::Simple';
		check_module_name($class);
		use_module($class, delete $p{store}{VERSION});
		$class->isa('DataStore::CAS')
			or croak "'$class' is not a valid CAS class\n";
		$p{store}= $class->new($p{store});
	}
	
	# coercion of scanner parameters to Scanner object
	$p{scanner} ||= { };
	if (!ref $p{scanner}) {
		$p{scanner}= { CLASS => $p{scanner} };
	}
	if (ref $p{scanner} eq 'HASH') {
		$p{scanner}= { %{$p{scanner}} }; # clone before we make changes
		my $class= delete $p{scanner}{CLASS} || 'DataStore::CAS::FS::Scanner';
		check_module_name($class);
		use_module($class, delete $p{scanner}{VERSION});
		$class->isa('DataStore::CAS::FS::Scanner')
			or croak "'$class' is not a valid Scanner class\n";
		$p{scanner}= $class->new($p{scanner});
	}

sub _load_config {
	my ($class, $backup_dir)= @_;
	my $cfg_file= File::Spec->catfile($backup_dir, $_config_filename);
	-f $cfg_file or die "Missing config file '$cfg_file'\n";
	-r $cfg_file or die "Permission denied for '$cfg_file'\n";
	my ($f, $json, $cfg);
	try {
		open($f, '<', $cfg_file)
			and defined do { local $/= undef; $json= <$f> }
			or die "$!\n";
		$cfg= JSON->new->utf8->relaxed->decode($json);
	}
	catch {
		my $err= "$_";
		chomp($err);
		# Clean up JSON's error messages a bit...
		if (defined($json) and ($err =~ /^(.*), at character offset (\d+)/)) {
			my $lineNum= scalar split /\n/, substr($json, 0, $2).'x';
			my $context= '"'.substr($json, $2, 10).'..."';
			$err= "$1 at line $lineNum near $context";
		}
		# and now explain to the user what's going on
		die "Unable to load config file '$cfg_file': $err\n";
	};
	return $cfg;
}

sub _initialize_backup {
	my ($class, $backup_dir, $cfg)= @_;
	my $cfg_file= File::Spec->catfile($backup_dir, $_config_filename);
	my $json= JSON->new->utf8->pretty->canonical->encode($cfg);
	my $fd;
	open($fd, ">", $cfg_file) && (print $fd $json) && close($fd)
		or die "Error writing configuration file '$cfg_file': $!\n";

	my $log_file= File::Spec->catfile($backup_dir, $_log_filename);
	open($fd, ">", $log_file) && close($fd)
		or die "Error writing log file '$log_file': $!\n";

	$class->_write_snapshot_index($backup_dir, []);
}

sub _write_snapshot_index {
	my ($class, $backup_dir, $snapshot_array)= @_;
	my $index_file= File::Spec->catfile($backup_dir, $_snapshots_filename);
	my $temp_file= $index_file . '.tmp';
	open my $fh, ">", $temp_file
		or die "Cannot write to '$temp_file': $!\n";
	print $fh "Timestampt\tHash\tComment\n";
	for my $snap (@$snapshot_array) {
		print $fh join("\t", @$snap)."\n";
	}
	close $fh
		or die "Cannot save '$temp_file': $!\n";
	rename $temp_file, $index_file
		or die "Cannot replace '$index_file': $!\n";
	1;
}

sub _read_snapshot_index {
	my ($class, $backup_dir)= @_;
	my $index_file= File::Spec->catfile($backup_dir, $_snashots_filename);
	open my $fh, "<", $index_file
		or die "Failed to read '$index_file': $!\n";
	my $header= <$fh>;
	$header eq "Timestampt\tHash\tComment\n"
		or die "Invalid snapshot index (wrong header): '$index_file'\n";
	my @entries;
	while (<$fh>) {
		chomp;
		my @fields= split /[\t]/, $_, 3;
		scalar(@fields) == 3
			or die "Invalid entry in '$index_file': \"$_\"\n";
		push @entries, \@fields;
	}
	\@entries;
}

1;
