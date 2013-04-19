package App::Casbak;
use Moo;
use Carp;
use Try::Tiny;
use JSON ();
use File::Spec;
use DataStore::CAS::FS;
use Module::Runtime;

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

=head1 ATTRIBUTES

=head2 backup_dir

Directory of the Casbak files, like the configuration and the log.
The CAS itself might or might not live within this directory.

=head2 config

Plain Perl data describing the configuration of this backup.  Config should
contain all the constructor parameters needed for the rest of the lazy-built
fields.

Config itself can be lazy-built by loading the config file in backup_dir.

=head2 cas

An instance of DataStore::CAS (a subclass, since CAS is an abstract class)

Lazy-built from ->config->{cas}

=head2 scanner

An instance of DataStore::CAS::FS::Scanner, used to import files from
the real filesystem to the virtual filesystem.

Lazy-built from ->config->{scanner}

=head2 extractor

An instance of DataStore::CAS::FS::Extractor, used to export virtual files
out to the real filesystem.

Lazy-built from ->config->{extractor}

=head2 snapshot_index

A data structure describing the snapshots available in this repository.
Pay no attention to this field; instead use "get_snapshot" and related
methods.

=head2 date_format

The date formatter used to parse dates from the command line, and render them
back to the user.

The default for date_format is chosen when the backup is initialized, by
App::Casbak::Cmd::Init.  It depends on which modules are available on your
system, but DateTime::Format::Natural is the first choice.

Lazy-built from ->config->{date_format}

=head2 config_filename

Path to the config file (includes backup_dir)

=head2 log_filename

Path to the log file (includes backup_dir)

=head2 snapshot_index_filename

Path to the snapshot index (includes backup_dir)

=cut

has backup_dir         => ( is => 'ro', required => 1, default => sub { '.' } );
has config             => ( is => 'lazy' );
has cas                => ( is => 'lazy' );
has scanner            => ( is => 'lazy' );
has extractor          => ( is => 'lazy' );
has snapshot_index     => ( is => 'lazy' );
has date_format        => ( is => 'lazy' );

sub config_filename {
	File::Spec->catfile($_[0]->backup_dir, 'casbak.conf.json');
}
sub log_filename {
	File::Spec->catfile($_[0]->backup_dir, 'casbak.log');
}
sub snapshot_index_filename {
	File::Spec->catfile($_[0]->backup_dir, 'casbak.snapshots.tsv');
}

=head1 METHODS

=head2 write_log_entry( $type, $message, $data )

=cut

sub write_log_entry {
	my ($self, $type, $message, $data)= @_;
	my ($sec,$min,$hour,$mday,$mon,$year)= gmtime(time);
	$message =~ tr/{}/()/; # message should never contain curly braces
	my $line= sprintf("%4d-%2d-%2dT%2d:%2d:%2dZ %s %s %s\n", $year, $mon, $mday, $hour, $min, $sec, $type, $message, JSON::json_encode($data));
	open (my $fh, '>>', $self->log_filename) or die "open(log): $!";
	(print $fh $line) or die "write(log): $!";
	close $fh or die "close(log): $!";
}

=head2 canonical_date( $date_thing [, $virtual_now [, $custom_parser ]] )

Takes a date in a variety of formats and returns a string in
"YYYY-MM-DDTHH:MM:SSZ" format.

  my $date= $self->canonical_date( "2000-01-01Z" );
  my $date= $self->canonical_date( "25D" );
  my $date= $self->canonical_date( time() );
  my $date= $self->canonical_date( DateTime->new() );

Allowed formats include canonical date strings or recognizable fractions of
one, unix 'time()' integers, DateTime objects, or relative notations like
"25D" (25 days ago), "2W" (2 weeks ago), "2Y" (2 years ago), or "3M"
(3 months ago).

While mostly for testing purposes, there are two optional arguments of
'$virtual_now' and '$custom_parser'.  You can use the first to make relative
dates from a specific point in time instead of the moment the method was
called.  This *only* applies to the relative dates of the form "25D" which
are handled directly, and not by the date parser.  Your date parser might or
might not have a mechanism to override "now()".  You can use $custom_parser
to override which object is used to parse non-standard date formats.
(the usual way to specify a parser is with the ->date_format attribute,
which can be saved in the config file)

=cut

my %_suffix_to_date_field= ( D => 'days', W => 'weeks', M => 'months', Y => 'years' );
sub canonical_date {
	my ($self, $date, $now, $parser)= @_;
	if (!ref $date) {
		# is it already in a valid format?
		if ($date =~ /^\d\d\d\d-\d\d(-\d\d(T\d\d:\d\d(:\d\d)?)?)?Z$/) {
			# supply missing parts of date, after clipping off 'Z'
			# We can return here without needing to create a DateTime object.
			return substr($date, 0, -1) . substr('0000-01-01T00:00:00Z', length($date)-1);
		}

		# is it a single integer? (unix epoch time)
		if ($date =~ /^\d+$/) {
			my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)= gmtime($date);
			# We can return here without needing to create a DateTime object.
			return sprintf('%4d-%02d-%02dT%02d:%02d:%02dZ', $year+1900, $mon+1, $mday, $hour, $min, $sec);
		}

		# is it a relative date?
		if ($date =~ /^(\d+)([DWMY])$/) {
			require DateTime;
			my $delta= DateTime::Duration->new($_suffix_to_date_field{$2} => $1, end_of_month => 'preserve');
			my $dt= !defined $now? DateTime->now(time_zone => 'floating')
				: !ref $now? DateTime->from_epoch(epoch => $now, time_zone => 'floating')
				: do { my $d= $now->clone; $d->set_time_zone('floating'); $d; };
			$date= $dt->subtract($delta);
		}
		# else we have to parse it
		else {
			$parser ||= $self->date_format;
			defined (my $dt= $parser->parse_datetime($date))
				or die "Invalid date: $date\n";
			# WHY!? WHY... *sigh*
			if ($parser->isa('DateTime::Format::Natural')) {
				die "Invalid date: $date\n"
					unless $parser->success;
			}
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

sub get_snapshot {
	my ($self, $target_date)= @_;
	my $array= $self->snapshot_index;
	
	return undef unless @$array;
	
	# if target_date is undef, return the latest snapshot
	return $array->[-1] unless defined $target_date;
	$target_date= $self->canonical_date($target_date);

	# Use binary search to find the snapshot.
	# Note we are comparing date strings in canonical _T_Z form (which sort alphabetically)
	my ($min, $max, $mid)= (-1, $#$array);
	while ($min < $max) {
		$mid= ($min+$max+1)>>1;
		if ($target_date ge $array->[$mid][0]) {
			$min= $mid;
		} else {
			$max= $mid-1;
		}
	}
	return undef if $max < 0;
	
	my $digest_hash= $array->[$min][1];
	my ($err, $snap);
	try {
		my $file= $self->cas->get($digest_hash);
		my $dir= DataStore::CAS::FS::Dir->new($file);
		$snap= App::Casbak::Snapshot->new($dir);
	}
	catch {
		chomp($err= $_);
	};
	defined $snap && return $snap;
	die "Unable to load snapshot from $digest_hash: $err\n";
}

sub get_snapshot_or_die {
	my ($self, $date)= @_;
	$self->get_snapshot($date)
		or die defined $date? "No snapshot existed on date $date\n" : "No snapshots recorded yet\n";
}
	
sub save_snapshot {
	my ($self, $root_entry, $metadata)= @_;
	
	# Default the timestamp to 'now'
	$metadata->{timestamp}= time
		unless defined $metadata->{timestamp};
	
	# Convert to standard date format
	$metadata->{timestamp}= $self->canonical_date($metadata->{timestamp});
	
	my $array= $self->snapshot_index;
	# Timestamps must be in cronological order
	# (we could insert-sort here, but people should only ever be adding "new" snapshots...)
	!scalar(@$array)
		or $metadata->{timestamp} ge $array->[-1][0]
		or croak "New timestamp '$metadata->{timestamp}' must be later than last recorded timestamp '$array->[-1][0]'";
	
	# Serialize the new snapshot and store it in the CAS
	my $encoded= DataStore::CAS::FS::Dir->SerializeEntries([ $root_entry ], $metadata);
	my $hash= $self->cas->put($encoded);
	
	# Append the new snapshot to the end of the snapshot index
	push @$array, [ $metadata->{timestamp}, $hash, $metadata->{comment} || '' ];
	# and write the new index file
	my $err;
	try { $self->_save_snapshot_index(); } catch { chomp($err= $_); };
	die "$err\n"
		."The entry that would have been written is: '$metadata->{timestamp} $hash'\n"
		if defined $err;
	1;
}

sub init {
	my ($class, $ctor_args)= @_;
	Trace('Casbak->init(): ', $ctor_args);

	# Default to current dir
	my $dir= defined $ctor_args->{backup_dir}? $ctor_args->{backup_dir} : ($ctor_args->{backup_dir}= '.');
	
	# Directory must exist and be empty
	my @entries= grep { $_ ne '.' && $_ ne '..' } <$dir/*>;
	-d $dir && -r $dir && -w $dir && -x $dir && 0 == @entries
		or croak "Backups may only be initialized in an empty writeable directory\n";

	# Record our own version in the config
	$ctor_args->{config}{VERSION}= $class->VERSION;

	# Make a copy of config, which will be saved to the config file
	require JSON;
	my $json= JSON->new->utf8->pretty->canonical->encode($ctor_args->{config});

	# If the CAS class supports 'create', we request it.
	my %validParams= map { $_ => 1 } $ctor_args->{config}{cas}[0]->_ctor_params;
	$ctor_args->{config}{cas}[2]{create}= 1
		if !defined $ctor_args->{config}{cas}[2]{create} and $validParams{create};

	# Initialize snapshotIndex to prevent it from getting loaded from a file that doesn't exist yet.
	$ctor_args->{snapshot_index}= [];

	# See if we can run the constructor
	my $self= $class->new($ctor_args);
	# Then call each of the lazy-built attributes to make sure they work
	$self->cas;
	$self->scanner;
	$self->extractor;
	$self->date_format;

	# No exceptions? Looks good.  So now we save it.

	# write config file
	$self->_overwrite_or_die($self->config_filename, $json);
	# initialize snapshot index
	$self->_save_snapshot_index();
	# initialize log file (by writing to it)
	$self->write_log_entry('INIT', "Backup initialized", JSON::json_decode($json));

	return $self;
}

#sub importTree {
#	my ($self, %p)= @_;
#	Trace('Casbak->importTree(): ', \%p);
#	
#	my ($srcPath, $dstPath, $rootEnt)= ($p{real}, $p{virt}, $p{root});
#
#	# The Root Dir::Entry defaults to the latest snapshot
#	unless (defined $rootEnt) {
#		my $snap= $self->getSnapshot();
#		$rootEnt= $snap->rootEntry
#			if defined $snap;
#	}
#	
#	# If we're starting from *nothing*, we fake the root Dir::Entry by
#	#   supplying the known hash of the canonical "Empty Directory"
#	$rootEnt= $self->cas->getEmptyDirHash()
#		unless defined $rootEnt;
#	
#	# Did they give us a proper DirEnt, or just a hash?
#	if (!ref $rootEnt) {
#		# They gave us a hash.  Convert to Dir::Entry.
#		$rootEnt= File::CAS::Dir::Entry->new( name => '', type => 'dir', hash => $rootEnt );
#	}
#	else {
#		ref($rootEnt)->isa('File::CAS::Dir::Entry')
#			or croak "Invalid 'root': must be File::CAS::Dir::Entry or digest string";
#		$rootEnt->type eq 'dir'
#			or croak "Root directory entry must describe a directory.";
#	}
#	
#	# Now get an array of Dir::Entry describing the entire destination path.
#	# Any missing directories will create generic/empty Dir::Entry objects.
#	my $err;
#	my $resolvedDest= $self->cas->resolvePathPartial($rootEnt, $dstPath, \$err);
#	croak "Can't resolve destination directory: $err"
#		if defined $err;
#	
#	# We always allow the final path element to be created/overwritten, but we only
#	# allow inbetween directories to be created if the user requested that feature.
#	if (@$resolvedDest > 1 and !defined $resolvedDest->[-2]->hash) {
#		$p{create_deep}
#			or croak "Destination path does not exist in backup: '$dstPath' ($err)";
#	}
#
#	# Now inspect the source entry in the real filesystem
#	my $srcEnt= $self->cas->scanner->scanDirEnt($srcPath)
#		or croak "Cannot stat '$srcPath'";
#	# It is probably a dir, but we also allow importing single files.
#	if ($srcEnt->{type} eq 'dir') {
#		my $hintDir;
#		if (defined $resolvedDest->[-1]) {
#			croak "Attempt to overwrite file with directory"
#				if (defined $resolvedDest->[-1]->type and $resolvedDest->[-1]->type ne 'dir');
#			$hintDir= $self->cas->getDir($resolvedDest->[-1]->hash)
#				if (defined $resolvedDest->[-1]->hash);
#		}
#		my $hash= $self->cas->putDir($srcPath, $hintDir);
#		
#		# When building the new dir entry, keep all source attrs except name,
#		#   but use destination entry attrs as defaults for attrs not set in $srcEnt
#		# Example:
#		#  $srcEnt = { name => 'foo', create_ts => 12345 };
#		#  $dstEnt = { name => 'bar', create_ts => 11111, unix_uid => 1002 };
#		#  $result = { name => 'bar', create_ts => 12345, unix_uid => 1002 };
#		my %attrs= %{$srcEnt->asHash};
#		delete $attrs{name};
#		%attrs= ( %{$resolvedDest->[-1]->asHash}, %attrs );
#		$resolvedDest->[-1]= File::CAS::Dir::Entry->new(%attrs);
#	}
#	else {
#		# We do not allow the root entry to be anything other than a directory.
#		(@$resolvedDest > 1)
#			or croak "Cannot store non-directory (type = ".$srcEnt->type.") as virtual root: '$srcPath'\n";
#		my $hash= $self->cas->putFile($srcPath);
#		$resolvedDest->[-1]= File::CAS::Dir::Entry->new( %{$srcEnt->asHash}, hash => $hash );
#	}
#	
#	# The final Dir::Entry in the list $resolvedDest has been modified.
#	# If it was not the root, then we need to walk up the tree modifying each directory.
#	while (@$resolvedDest > 1) {
#		my $newEnt= pop @$resolvedDest;
#		my $dirEnt= $resolvedDest->[-1];
#		my $dir= $self->cas->getDir($dirEnt->hash);
#		$dir= $self->cas->mergeDir($dir, [ $newEnt ] );
#		my $hash= $self->cas->putDir($dir);
#		$resolvedDest->[-1]= File::CAS::Dir::Entry->new( %{$dirEnt->asHash}, hash => $hash );
#	}
#	
#	# Return the new root Dir::Entry (caller will likely save this as a snapshot)
#	return $resolvedDest->[0];
#}

sub _build_config {
	my $self= shift;
	my $cfg_file= $self->config_filename;
	-f $cfg_file or die "Missing config file '$cfg_file'\n";
	-r $cfg_file or die "Permission denied for '$cfg_file'\n";
	my ($f, $json, $cfg);
	try {
		$json= $self->_slurp_or_die($cfg_file);
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
	# Run a version check
	__PACKAGE__->VERSION($cfg->{VERSION});
	return $cfg;
}

sub _get_module_constructor_args {
	my ($self, $field_name, $thing_name, $required_ancestor, $required_method)= @_;
	my $args= $self->config->{$field_name};
	defined ($args)
		or die "No $thing_name was passed to the constructor, and config.$field_name is missing\n";
	ref $args eq 'ARRAY' and 3 <= @$args
		or die "No $thing_name was passed to the constructor, and config.$field_name is invalid\n";
	my ($class, $version, @args)= @$args;

	# Load the class, and possibly check version.
	check_module_name($class);
	require_module($class);
	$class->VERSION($version)
		if defined $version;

	# Check features of the loaded class
	(!defined $required_ancestor || $class->isa($required_ancestor))
	&& (!defined $required_method || $class->can($required_method))
		or die "'$class' is not a valid $thing_name class\n";
	
	# use clone of $args
	# Could use dclone, but we've already loaded the JSON module, and thats where it came from anyway
	return ( $class, @{JSON::json_decode(JSON::json_encode(\@args))} );
}

sub _build_cas {
	my $self= shift;
	my ($class, @args)= $self->_get_module_constructor_args('cas', 'CAS', 'DataStore::CAS', undef);

	# If the constructor has a 'path' parameter and it is relative, we convert
	# it to be relative to backup_dir.
	if (@args == 1 && ref $args[0] eq 'HASH' && defined $args[0]->{path}) {
		$args[0]->{path}= File::Spec->rel2abs($args[0]->{path}, $self->backup_dir)
			unless File::Spec->file_name_is_absolute($args[0]->{path});
	}

	$class->new(@args);
}

sub _build_scanner {
	my $self= shift;
	my ($class, @args)= $self->_get_module_constructor_args('scanner', 'Scanner', 'DataStore::CAS::FS::Scanner', undef);
	$class->new(@args);
}

sub _build_extractor {
	my $self= shift;
	my ($class, @args)= $self->_get_module_constructor_args('extractor', 'Extractor', 'DataStore::CAS::FS::Extractor', undef);
	$class->new(@args);
}

sub _build_snapshot_index {
	$_[0]->_read_snapshot_index($_[0]->snapshot_index_filename);
}
sub _save_snapshot_index {
	$_[0]->_write_snapshot_index($_[0]->snapshot_index_filename);
}

sub _build_date_format {
	my $self= shift;
	my ($class, @args)= $self->_get_module_constructor_args('date_format', 'Date Format', undef, 'parse_datetime');
	$class->new(@args);
}

sub _slurp_or_die {
	my ($class, $filename)= @_;
	local $/= undef;
	my $f;
	open($f, '<', $filename)
		&& defined(my $data= <$f>)
		&& close($f)
		or die "read($filename): $!\n";
	$data;
}
sub _overwrite_or_die {
	my ($class, $filename, $data)= @_;
	my $f;
	open($f, ">", $filename)
		&& (print $f $data)
		&& close($f)
		or die "write($filename): $!\n";
	1;
}

sub _write_snapshot_index {
	my ($class, $index_file, $snapshot_array)= @_;
	# Build a string of TSV (tab separated values)
	my $data= join '',
		"Timestampt\tHash\tComment\n",
		map { join("\t", @$_)."\n" } @$snapshot_array;
	# Write it to a temp file, and then rename to the official name
	my $temp_file= $index_file . '.tmp';
	$class->_overwrite_or_die($temp_file, $data);
	rename $temp_file, $index_file
		or die "Cannot replace '$index_file': $!\n";
	1;
}

sub _read_snapshot_index {
	my ($class, $index_file)= @_;
	my $tsv= $class->_slurp_or_die($index_file);
	my @lines= split /\r?\n/, $tsv;
	my $header= shift @lines;
	$header eq "Timestampt\tHash\tComment"
		or die "Invalid snapshot index (wrong header): '$index_file'\n";
	my @entries;
	for (@lines) {
		my @fields= split /\t/, $_, 3;
		scalar(@fields) == 3
			or die "Invalid entry in '$index_file': \"$_\"\n";
		push @entries, \@fields;
	}
	\@entries;
}

1;
