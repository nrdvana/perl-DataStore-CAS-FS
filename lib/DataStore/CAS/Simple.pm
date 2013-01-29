package DataStore::CAS::Simple;
use 5.008;
use strict;
use warnings;
use Carp;
use Try::Tiny;

use parent 'DataStore::CAS';
our $VERSION = '0.01';

=head1 NAME

DataStore::CAS::Simple - Simple file/directory based CAS implementation

=head1 DESCRIPTION

This implementation of DataStore::CAS uses a directory tree where the
filenames are the hexadecimal value of the digest hashes.  The files are
placed into directories named with a prefix of the digest hash to prevent
too many entries in the same directory (which is actually only a concern
on certain filesystems).

Opening a DataStore::CAS::File returns a real perl filehandle, and copying
a File object from one instance to another is optimized by hard-linking the
underlying file.

  # This is particularly fast:
  $cas1= DataStore::CAS::Simple( path => 'foo' );
  $cas2= DataStore::CAS::Simple( path => 'bar' );
  $cas1->put( $cas2->get( $hash ) );

This class does not perform any sort of optimization on the storage of the
content, neither by combining commom sections of files nor by running common
compression algorithms on the data.

TODO: write DataStore::CAS::Compressor or DataStore::CAS::Splitter
for those features.

=cut

use Digest;
use File::Spec::Functions 'catfile', 'catdir', 'canonpath';
use File::Copy;
use File::Temp 'tempfile';

=head1 ATTRIBUTES

=head2 path

Read-only.  The filesystem path where the store is rooted.

=head2 digest

Read-only.  Algorithm used to calculate the hash values.  This can only be
set in the constructor when a new store is being created.  Default is 'SHA-1'.

=head2 directory_fanout

Read-only.  Returns arrayref of pattern used to split digest hashes into
directories.  Each digit represents a number of characters from the front
of the hash which then become a directory name.

For example, "[ 2, 2 ]" would turn a hash of "1234567890" into a path of
"12/34/567890".

=head2 directory_fanout_regex

Read-only.  A regex-ref which splits a digest hash into the parts needed
for the path name.  A fanout of "[ 2, 2 ]" creates a regex of "/(.{2})(.{2})(.*)/"

=head2 copy_buffer_size

Number of bytes to copy at a time when saving data from a filehandle to the
CAS.  This is a performance hint, and the default is usually fine.

=cut

sub path             { $_[0]{path} }

sub directory_fanout { [ @{$_[0]{directory_fanout}} ] }

sub directory_fanout_regex {
	$_[0]{directory_fanout_regex} ||= do {
		my $regex= join('', map { "(.{$_})" } $_[0]->directory_fanout ).'(.*)';
		qr/$regex/;
	};
}

sub copy_buffer_size { $_[0]{copy_buffer_size}= $_[1] if (@_ > 1); $_[0]{copy_buffer_size} || 256*1024 }

=head1 METHODS

=head2 new( \%params | %params )

Constructor.  It will load (and possibly create) a CAS Store.

'path' points to the cas directory.  Trailing slashes don't matter.
It is a good idea to use an absolute path in case you 'chdir' later.

'copy_buffer_size' initializes the respective attribute.

The 'digest' attribute can only be initialized if the store is being created.
Otherwise, it is loaded from the store's configuration.

If 'create' is specified, and 'path' refers to an empty directory, a fresh store
will be initialized.

'ignore_version' allows you to load a Store even if it was created with a newer
version of the ::CAS::Simple package that you are now using.  (or a different
package entirely)

To dynamically find out which parameters the constructor accepts,
call $class->_ctor_params(), which returns a list of valid keys.

=cut

# We inherit 'new', and implement '_ctor'.  The parameters to _ctor are always a hash.

our @_ctor_params= qw: path digest copy_buffer_size create ignore_version directory_fanout :;
sub _ctor_params { ($_[0]->_ctor_params, @_ctor_params); }
sub _ctor {
	my ($class, $params)= @_;
	my %p= map { $_ => delete $params->{$_} } @_ctor_params;

	# Check for invalid params
	croak "Invalid parameter: ".join(', ', keys %$params)
		if (keys %$params);

	# extract constructor flags which don't belong in attributes
	my $create= delete $p{create};
	my $ignore_version= delete $p{ignore_version};
	my $default_digest= delete $p{digest} || 'SHA-1';
	my $default_fanout= delete $p{directory_fanout} || [ 1, 2 ];
	
	# Path is required, and must be a directory
	croak "Parameter 'path' is required"
		unless defined $p{path};
	croak "Path '$p{path}' is not a directory"
		unless -d $p{path};
	
	# Check directory
	unless (-f catfile($p{path}, 'conf', 'VERSION')
		and -f catfile($p{path}, 'conf', 'DIGEST') )
	{
		croak "Path does not appear to be a valid CAS : '$p{path}'"
			unless $create;

		# Here, we are creating a new CAS directory
		my $self= bless {
				%p,
				digest => $default_digest,
				directory_fanout => $default_fanout
			}, $class;
		# But first, make sure we are creating in an empty dir
		croak "Directory '$path' is not empty"
			unless $self->_is_dir_empty($p{path});
		# And make sure the fanout isn't insane
		$self->_validate_fanout;
		# Then, write out the configuration and initialize various things.
		$self->_initialize_store();
		
		# We could just use that '$self', but we want to double-check our initialization
		#  by continuing through the regular constructor code path.
	}
	
	my $self= bless \%p, $class;
	
	$self->_load_config();

	try {
		$self->_check_version();
	}
	catch {
		$ignore_version ? warn($_) : die($_);
	};
	
	# Properly initialized CAS will always contain an entry for the empty string
	$self->{hash_of_null}= $self->_new_digest->hexdigest();
	croak "CAS dir '".$self->path."' is missing a required file (has it been initialized?)"
		unless $self->validate($self->hash_of_null);
	
	return $self;
}

# Called during constrctor when creating a new Store directory.
sub _initialize_store {
	my ($self)= @_;
	my $conf_dir= catdir($self->path, 'conf');
	mkdir($conf_dir) or croak "mkdir($conf_dir): $!";
	$self->_write_config_setting('VERSION', ref($self).' '.$VERSION."\n");
	$self->_write_config_setting('DIGEST', $self->digest."\n");
	$self->_write_config_setting('FANOUT', join(' ', @{$self->directory_fanout})."\n");
	$self->put('');
}

sub _is_dir_empty {
	my ($self, $path)= @_;
	opendir(my $dh, $path)
		or die "opendir($path): $!";
	my @entries= grep { $_ ne '.' and $_ ne '..' } readdir($dh);
	closedir($dh);
	return @entries == 0;
}

# In the name of being "Simple", I decided to just read and write
# raw files for each parameter instead of using JSON or YAML.
# It is not expected that this module will have very many options.
# Subclasses will likely use YAML.

sub _write_config_setting {
	my ($self, $fname, $content)= @_;
	my $path= catfile($self->path, 'conf', $fname);
	open(my $f, '>', $path)
		or croak "Failed to open '$path' for writing: $!\n";
	(print $f $content) && (close $f)
		or croak "Failed while writing '$path': $!\n";
}
sub _read_config_setting {
	my ($self, $fname)= @_;
	my $path= catfile($self->path, 'conf', $fname);
	open(my $f, '<', $path)
		or croak "Failed to read '$path' : $!\n";
	local $/= undef;
	return <$f>;
}

# This method loads the digest and fanout configuration and validates it
# It is called during the constructor.
sub _load_config {
	my $self= shift;

	# Get the digest algorithm name
	chomp( $self->{digest}= $self->_read_config_setting('DIGEST') );
	# Check for digest algorithm availability
	my $found= ( try { $self->_new_digest; 1; } catch { 0; } )
		or croak "Digest algorithm '".$self->digest."' is not available on this system.\n";

	# Get the fanout
	$self->{directory_fanout}= [ split /\s+/, $self->_read_config_setting('FANOUT') ];
	$self->_validate_fanout;

	return 1;
}

sub _validate_fanout {
	my $self= shift
	# Sanity check on the fanout
	my $digits= 0;
	for (@{ $self->directory_fanout }) {
		$digits+= $_;
		croak "Too large fanout in one directory ($_)" if $_ > 3;
	}
	croak "Too many digits of fanout! ($digits)" if $digits > 5;
}

# This method loads the version the store was initialized with
#  and checks to see if we are compatible with it.
sub _check_version {
	my $self= shift;

	# Version str is "$PACKAGE $VERSION\n", where version is a number but might have a string suffix on it
	my $version_str= $self->_read_config_setting('VERSION');
	($version_str =~ /^([A-Za-z0-9:_]+) ([0-9.]+)/)
		or croak "Invalid version string in storage dir '".$self->path."'\n";

	# Check $PACKAGE
	($1 eq ref($self))
		or croak "Class mismatch: storage dir was created with $1 but you're trying to access it with ".ref($self)."\n";

	# Check $VERSION
	($2 > 0 and $2 <= $VERSION)
		or croak "Storage dir '".$self->path."' was created by version $2 of ".ref($self).", but this is only $VERSION\n";

	return 1;
}

=head2 get( $digest_hash )

Returns a DataStore::CAS::File object for the given hash, if the hash
exists in storage. Else, returns undef.

=cut

sub get {
	my ($self, $hash)= @_;
	my $fname= catfile($self->_path_for_hash($hash));
	return undef
		unless (my ($size, $blksize)= (stat $fname)[7,11]);
	return bless {
		# required
		store      => $self,
		hash       => $hash,
		size       => $size,
		# extra info
		block_size => $blksize,
		local_file => $fname,
	}, 'DataStore::CAS::Simple::File';
}

sub put_file {
	my ($self, $file, $flags)= @_;
	$flags ||= {};

	# Here is where we detect opportunity to perform optimized hard-linking
	#  when copying to and from CAS implementations which are backed by
	#  plain files.
	if (ref $file and ref($file)->isa('DataStore::CAS::File')
		and $file->can('local_file') and length $file->local_file
	) {
		$flags->{link_from_local_file}= $file->local_file;
		$flags->{known_hash}= $file->hash
			if ($file->store->digest eq $self->digest);
	}

	# Else use the default implementation which opens and reads the file.
	(shift)->SUPER::put_file(@_);
}

sub _write_all_or_die {
	my ($fh, $data)= @_;
	my $wrote;
	my $ofs= 0;
	while ($ofs < length($data)) {
		$wrote= syswrite($fh, $data, length($data) - $ofs, $ofs);
		if ($wrote) {
			$ofs+= $wrote;
		} else {
			croak "$!"
				unless !defined($wrote) && ($!{EINTR} || $!{EAGAIN});
		}
	}
}

sub put_handle {
	my ($self, $data, $flags)= @_;
	my $hardlink_source= $flags->{link_from_local_file};
	my $dest_hash= $flags->{verify_hash}? undef : $flags->{known_hash};
	my $scalar= $flags->{source_is_plain_scalar};
	my $dest_name;

	# If we know the hash...
	if (defined $dest_hash) {
		my $dest_name= $self->_path_for_hash($dest_hash);

		# If we know the hash, and we have it already, nevermind
		return $dest_hash
			if -f $dest_name;

		# If we know the hash, and we want to hard-link, try it
		if (defined $hardlink_source) {
			# dry-run succeeds automatically.
			# we check for missing directories after the first failure,
			#   in the spirit of keeping the common case fast.
			if ($flags->{dry_run} or link( $source_file, $dest_name )
				or ($self->_add_missing_path($dest_hash) and link( $source_file, $dest_name ))
			) {
				# record that we added a new hash, if stats enabled.
				if ($flags->{stats}) {
					$flags->{stats}{new_file_count}++;
					push @{ $flags->{stats}{new_files} ||= [] }, $dest_hash;
				}
				return $dest_hash;
			}
			# else we can't hard-link for some reason
			$hardlink_source= undef;
		}
		# here, we need to copy the file, so go to the loop below.
	}

	# Create a temp file to write to
	my ($dest_fh, $temp_name)= tempfile( 'temp-XXXXXXXX', DIR => $self->path )
		unless $flags->{dry_run};

	# If we don't know the destination hash, but we want to attempt hard-linking,
	# try hard-linking to ->path and then later rename it to the dest_name
	if ($hardlink_source && !$flags->{dry_run}) {
		if (link( $hardlink_source, $temp_name."-lnk" )) {
			# success - we don't need to copy the file, just checksum it and rename.
			close($dest_fh);
			$dest_fh= undef;
			rename($temp_name."-lnk", $temp_name)
				or croak "rename(-> $temp_name): $!";
		}
		# else we failed to hardlink, and will use $dest_fh
	}

	try {
		my $digest= $self->_new_digest
			unless defined $dest_hash;
		binmode $dest_fh
			if defined $dest_fh;

		# Read chunks of the stream, and either hash or save them or both.
		my $buf;
		while(1) {
			my $got= sysread($data, $buf, $self->copy_buffer_size);
			if ($got) {
				# hash it (maybe)
				$digest->add($buf) unless defined $dest_hash;
				# then write to temp file (maybe)
				_write_all_or_die($dest_fh, $buf) if defined $dest_fh;
			} elsif (!defined $got) {
				next if ($!{EINTR} || $!{EAGAIN});
				croak "while reading input: $!";
			} else {
				last;
			}
		}

		if ($dest_fh) {
			close $dest_fh
				or croak "while saving '$temp_name': $!";
		}

		unless (defined $dest_hash) {
			$dest_hash= $digest->hexdigest;
			$dest_name= $self->_path_for_hash($dest_hash);
		}
		
		if (-f $dest_name) {
			# we already have it
			unlink $temp_name;
		} else {
			# move it into place
			# we check for missing directories after the first failure,
			#   in the spirit of keeping the common case fast.
			$flags->{dry_run}
				or rename($temp_name, $dest_name)
				or ($self->_add_missing_path($dest_hash) and rename($temp_name, $dest_name))
				or croak "rename(-> $dest_name): $!";
			# record that we added a new hash, if stats enabled.
			if ($flags->{stats}) {
				$flags->{stats}{new_file_count}++;
				push @{ $flags->{stats}{new_files} ||= [] }, $dest_hash;
			}
		}
	}
	finally {
		close $dest_fh if defined $dest_fh;
		unlink $temp_name if defined $temp_name;
	};

	return $dest_hash;
}

sub validate {
	my ($self, $hash)= @_;

	my $path= $self->_path_for_hash($hash);
	return undef unless -f $path;

	open (my $fh, "<:raw", $path)
		or return 0; # don't die.  Errors mean "not valid".
	my $hash2= try { $self->_new_digest->addfile($fh)->hexdigest } catch { '' };
	return ($hash eq $hash2? 1 : 0);
}

sub file_open {
	my ($self, $file, $flags)= @_;
	my $mode= '<';
	$mode .= ':'.$flags->{layer} if ($flags && $flags->{layer});
	open my $fh, $mode, $file->local_file
		or croak "open: $!";
	return $fh;
}

sub _new_digest {
	Digest->new((shift)->digest);
}

sub _path_for_hash {
	my ($self, $hash)= @_;
	return catfile($self->path, ($hash =~ $self->directory_fanout_regex));
}

sub _add_missing_path {
	my ($self, $hash)= @_;
	my $str= $self->path;
	my @parts= ($hash =~ $self->directory_fanout_regex);
	pop @parts; # discard filename
	for (@parts) {
		$str= catdir($str, $_);
		next if -d $str;
		mkdir($str) or croak "mkdir($str): $!";
	}
	1;
}

package DataStore::CAS::Simple::File;
use strict;
use warnings;
use parent 'DataStore::CAS::File';

sub local_file { $_[0]{local_file} }
sub block_size { $_[0]{block_size} }

1; # End of File::CAS::Store::Simple
