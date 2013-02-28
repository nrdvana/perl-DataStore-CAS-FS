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

use Digest ();
use File::Spec::Functions 'catfile', 'catdir', 'canonpath';
use File::Temp ();

=head1 ATTRIBUTES

=head2 path

Read-only.  The filesystem path where the store is rooted.

=head2 digest

Read-only.  Algorithm used to calculate the hash values.  This can only be
set in the constructor when a new store is being created.  Default is 'SHA-1'.

=head2 fanout

Read-only.  Returns arrayref of pattern used to split digest hashes into
directories.  Each digit represents a number of characters from the front
of the hash which then become a directory name.

For example, "[ 2, 2 ]" would turn a hash of "1234567890" into a path of
"12/34/567890".

=head2 copy_buffer_size

Number of bytes to copy at a time when saving data from a filehandle to the
CAS.  This is a performance hint, and the default is usually fine.

=head2 storage_format_version

Hashref of version information about the modules that created the store.
Newer library versions can determine whether the storage is using an old
format using this information.

=head2 _fanout_regex

Read-only.  A regex-ref which splits a digest hash into the parts needed
for the path name.
A fanout of "[ 2, 2 ]" creates a regex of "/(.{2})(.{2})(.*)/"


=cut

sub path   { $_[0]{path} }

sub fanout { [ @{$_[0]{fanout}} ] }

sub _fanout_regex {
	$_[0]{_fanout_regex} ||= do {
		my $regex= join('', map { "(.{$_})" } @{$_[0]->fanout} ).'(.*)';
		qr/$regex/;
	};
}

sub copy_buffer_size {
	$_[0]{copy_buffer_size}= $_[1] if (@_ > 1);
	$_[0]{copy_buffer_size} || 256*1024
}

=head1 METHODS

=head2 new( \%params | %params )

Constructor.  It will load (and possibly create) a CAS Store.

If 'create' is specified, and 'path' refers to an empty directory, a fresh
store will be initialized.  If 'create' is specified and the directory is
already a valid CAS, 'create' is ignored, as well as 'digest' and
'fanout'.

'path' points to the cas directory.  Trailing slashes don't matter.
You might want to use an absolute path in case you 'chdir' later.

'copy_buffer_size' initializes the respective attribute.

The 'digest' and 'fanout' attributes can only be initialized if
the store is being created.
Otherwise, it is loaded from the store's configuration.

'ignore_version' allows you to load a Store even if it was created with a
newer version of the ::CAS::Simple package that you are now using.
(or a different package entirely)

To dynamically find out which parameters the constructor accepts,
call $class->_ctor_params(), which returns a list of valid keys.

=cut

# We inherit 'new', and implement '_ctor'.  The parameters to _ctor are always a hash.

our @_ctor_params= qw: path copy_buffer_size create ignore_version :;
our @_create_params= qw: digest fanout _notest :;
sub _ctor_params { ($_[0]->_ctor_params, @_ctor_params, @_create_params); }
sub _ctor {
	my ($class, $params)= @_;
	my %p= map { $_ => delete $params->{$_} } @_ctor_params;
	my %create= map { $_ => delete $params->{$_} } @_create_params;

	# Check for invalid params
	croak "Invalid parameter: ".join(', ', keys %$params)
		if (keys %$params);

	# extract constructor flags which don't belong in attributes
	my $create= delete $p{create};
	my $ignore_version= delete $p{ignore_version};
	
	# Path is required, and must be a directory
	croak "Parameter 'path' is required"
		unless defined $p{path};
	croak "Path '$p{path}' is not a directory"
		unless -d $p{path};
	
	# Check directory
	unless (-f catfile($p{path}, 'conf', 'VERSION')) {
		croak "Path does not appear to be a valid CAS : '$p{path}'"
			unless $create;

		# Here, we are creating a new CAS directory
		$class->create_store( path => $p{path}, %create );
	}

	my $cfg= $class->_load_config($p{path}, { ignore_version => $ignore_version });
	%p= (%p, %$cfg); # merge new parameters loaded from store config

	my $self= $class->SUPER::_ctor($params);
	%$self= (%$self, %p); # merge our attributes with parent class
	
	unless ($create{_notest}) {
		# Properly initialized CAS will always contain an entry for the empty string
		$self->{hash_of_null}= $self->_new_digest->hexdigest();
		croak "CAS dir '".$self->path."' is missing a required file"
		     ." (has it been initialized?)"
			unless $self->validate($self->hash_of_null);
	}

	return $self;
}

# Create a new store at a specified path.
# Also called during constrctor when { create => 1 }
sub create_store {
	my $class= shift;
	my %params= (@_ == 1? %{$_[0]} : @_);
	
	defined $params{path} or croak "Missing required param 'path'";
	-d $params{path} or croak "Directory '$params{path}' does not exist";
	# Make sure we are creating in an empty dir
	croak "Directory '$params{path}' is not empty\n"
		unless $class->_is_dir_empty($params{path});

	$params{digest} ||= 'SHA-1';
	# Make sure the algorithm is available
	my $found= ( try { defined $class->_new_digest($params{digest}); } catch { 0; } )
		or croak "Digest algorithm '".$params{digest}."'"
		        ." is not available on this system.\n";

	$params{fanout} ||= [ 1, 2 ];
	# make sure the fanout isn't insane
	$params{fanout}= $class->_parse_fanout(join(' ',@{$params{fanout}}));

	my $conf_dir= catdir($params{path}, 'conf');
	mkdir($conf_dir) or croak "mkdir($conf_dir): $!";
	$class->_write_config_setting($params{path}, 'VERSION', $class.' '.$VERSION."\n");
	$class->_write_config_setting($params{path}, 'digest', $params{digest}."\n");
	$class->_write_config_setting($params{path}, 'fanout', join(' ', @{$params{fanout}})."\n");
	
	# Finally, we add the null string to an instance of the CAS.
	$class->new(path => $params{path}, _notest => 1)->put('');
}

# This method loads the digest and fanout configuration and validates it
# It is called during the constructor.
sub _load_config {
	my ($class, $path, $flags)= @_;
	my %params;
	
	# Version str is "$PACKAGE $VERSION\n", where version is a number but might have a
	#   string suffix on it
	$params{storage_format_version}=
		$class->_parse_version($class->_read_config_setting($path, 'VERSION'));
	unless ($flags->{ignore_version}) {
		while (my ($pkg, $ver)= each %{$params{storage_format_version}}) {
			defined *{$pkg.'::VERSION'}
				or croak "Class mismatch: storage dir was created using $pkg"
				        ." but that package is not loaded now\n";
			my $curVer= do { no strict 'refs'; ${$pkg.'::VERSION'} };
			($ver > 0 and $ver <= $curVer)
				or croak "Version mismatch: storage dir was created using"
				        ." version '$ver' of $pkg but this is only $curVer\n";
		}
	}

	# Get the digest algorithm name
	$params{digest}=
		$class->_parse_digest($class->_read_config_setting($path, 'digest'));
	# Check for digest algorithm availability
	my $found= ( try { $class->_new_digest($params{digest}); 1; } catch { 0; } )
		or croak "Digest algorithm '".$params{digest}."'"
		        ." is not available on this system.\n";

	# Get the directory fan-out specification
	$params{fanout}=
		$class->_parse_fanout($class->_read_config_setting($path, 'fanout'));

	return \%params;
}

sub _is_dir_empty {
	my (undef, $path)= @_;
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
	my (undef, $path, $name, $content)= @_;
	$path= catfile($path, 'conf', $name);
	open(my $f, '>', $path)
		or croak "Failed to open '$path' for writing: $!\n";
	(print $f $content) && (close $f)
		or croak "Failed while writing '$path': $!\n";
}
sub _read_config_setting {
	my (undef, $path, $name)= @_;
	$path= catfile($path, 'conf', $name);
	open(my $f, '<', $path)
		or croak "Failed to read '$path' : $!\n";
	local $/= undef;
	my $str= <$f>;
	defined $str and length $str or croak "Failed to read '$path' : $!\n";
	return $str;
}

sub _parse_fanout {
	my (undef, $fanout)= @_;
	chomp($fanout);
	my @fanout;
	# Sanity check on the fanout
	my $total_digits= 0;
	for (split /\s+/, $fanout) {
		($_ =~ /^(\d+)$/) or croak "Invalid fanout spec";
		push @fanout, $1;
		$total_digits+= $1;
		croak "Too large fanout in one directory ($1)" if $1 > 3;
	}
	croak "Too many digits of fanout! ($total_digits)" if $total_digits > 5;
	return \@fanout;
}

sub _parse_digest {
	my (undef, $digest)= @_;
	chomp($digest);
	($digest =~ /^(\S+)$/)
		or croak "Invalid digest algorithm name: '$digest'\n";
	return $1;
}

sub _parse_version {
	my (undef, $version)= @_;
	my %versions;
	for my $line (split /\n/, $version) {
		($line =~ /^([A-Za-z0-9:_]+) ([0-9.]+)/)
			or croak "Invalid version string: '$line'\n";
		$versions{$1}= $2;
	}
	return \%versions;
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

	# Here is where we detect opportunity to perform optimized hard-linking
	#  when copying to and from CAS implementations which are backed by
	#  plain files.
	if (ref $file and ref($file)->isa('DataStore::CAS::File')
		and $file->can('local_file') and length $file->local_file
	) {
		$flags= { $flags? %$flags : () }; # make a copy
		$flags->{link_from_local_file}= $file->local_file;
		$flags->{known_hash}= $file->hash
			if ($file->store->digest eq $self->digest);
	}

	# Else use the default implementation which opens and reads the file.
	$self->SUPER::put_file($file, $flags);
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
	my ($self, $fh, $flags)= @_;
	my $hardlink_source= $flags->{link_from_local_file};
	my $dest_hash= $flags->{verify_hash}? undef : $flags->{known_hash};
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
			if ($flags->{dry_run} or link( $hardlink_source, $dest_name )
				or ($self->_add_missing_path($dest_hash) and link( $hardlink_source, $dest_name ))
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
	my ($temp_file, $need_copy);
	if (!$flags->{dry_run}) {
		$temp_file= File::Temp->new( TEMPLATE => 'temp-XXXXXXXX', DIR => $self->path );
		binmode $temp_file;
		$need_copy= 1;
		# If we don't know the destination hash, but we want to attempt hard-linking,
		# try hard-linking to ->path and then later rename it to the dest_name
		if ($hardlink_source) {
			if (link( $hardlink_source, $temp_file."-lnk" )) {
				# success - we don't need to copy the file, just checksum it and rename.
				# We cheat, and let the File::Temp object unlink us if an
				#  exception occurs before we do the final rename().
				unlink($temp_file);
				rename($temp_file."-lnk", $temp_file)
					or croak "rename(-> $temp_file): $!";
			}
			# else we failed to hardlink, and will use $dest_fh
		}
	}

	my $digest= $self->_new_digest
		unless defined $dest_hash;

	# Read chunks of the stream, and either hash or save them or both.
	my $buf;
	my $is_real_fd= (defined fileno($fh) and fileno($fh) >= 0);
	binmode $fh;
	while(1) {
		my $got= $is_real_fd?
			sysread($fh, $buf, $self->copy_buffer_size)
			: read($fh, $buf, $self->copy_buffer_size);
		if ($got) {
			# hash it (maybe)
			$digest->add($buf) unless defined $dest_hash;
			# then write to temp file (maybe)
			_write_all_or_die($temp_file, $buf) if $need_copy;
		} elsif (!defined $got) {
			next if ($!{EINTR} || $!{EAGAIN});
			croak "while reading input: $!";
		} else {
			last;
		}
	}

	close $temp_file
		or croak "while saving '$temp_file': $!";

	unless (defined $dest_hash) {
		$dest_hash= $digest->hexdigest;
		$dest_name= $self->_path_for_hash($dest_hash);
	}
	
	# Only if we don't have it yet
	unless (-f $dest_name) {
		# move it into place
		# we check for missing directories after the first failure,
		#   in the spirit of keeping the common case fast.
		$flags->{dry_run}
			or rename($temp_file, $dest_name)
			or ($self->_add_missing_path($dest_hash) and rename($temp_file, $dest_name))
			or croak "rename($temp_file => $dest_name): $!";
		# record that we added a new hash, if stats enabled.
		if ($flags->{stats}) {
			$flags->{stats}{new_file_count}++;
			push @{ $flags->{stats}{new_files} ||= [] }, $dest_hash;
		}
	}

	return $dest_hash;
}

sub validate {
	my ($self, $hash)= @_;

	my $path= $self->_path_for_hash($hash);
	return undef unless -f $path;

	open (my $fh, "<:raw", $path)
		or return 0; # don't die.  Errors mean "not valid".
	my $hash2= try { $self->_new_digest->addfile($fh)->hexdigest } catch {''};
	return ($hash eq $hash2? 1 : 0);
}

sub open_file {
	my ($self, $file, $flags)= @_;
	my $mode= '<';
	$mode .= ':'.$flags->{layer} if ($flags && $flags->{layer});
	open my $fh, $mode, $file->local_file
		or croak "open: $!";
	return $fh;
}

# This can be called as class or instance method.
# When called as an instance method, '$digest_name' is mandatory,
#   otherwise it is unneeded.
sub _new_digest {
	my ($self, $digest_name)= @_;
	Digest->new($digest_name || $self->digest);
}

sub _path_for_hash {
	my ($self, $hash)= @_;
	return catfile($self->path, ($hash =~ $self->_fanout_regex));
}

sub _add_missing_path {
	my ($self, $hash)= @_;
	my $str= $self->path;
	my @parts= ($hash =~ $self->_fanout_regex);
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
