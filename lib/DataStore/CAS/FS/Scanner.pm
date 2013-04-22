package DataStore::CAS::FS::Scanner;
use 5.008;
use strict;
use warnings;
use Carp;
use Try::Tiny;
use Fcntl ':mode';
use File::Spec::Functions 'catfile', 'catdir', 'splitpath', 'catpath';

our $VERSION= 0.01;

=head1 NAME

DataStore::CAS::FS::Scanner

=head1 DESCRIPTION

=head1 ATTRIBUTES

=head2 dir_format

Read/write.  Directory format to use when encoding directories.

Directories can be recorded with varying levels of metadata
(determined by the scanner) and encoded in a variety of formats which
are optimized for various uses.

The format strings are registered by DirCodec classes when loaded.
See L<DataStore::CAS::FS::DirCodec>.
Built-in formats are 'universal', 'minimal', or 'unix'.

Calls to store_dir will encode directories in this format.  The default is
'universal'.

=head2 filter

Read/write.  This optional coderef (which may be an object with overloaded
function operator) filters out files that you wish to ignore when walking
the physical filesystem.

It is passed 3 arguments: The name, the full path, and the results of 'stat'
as a blessed arrayref.  You are also guaranteed that stat was called
on this file immediately preceeding, so you may use code like "-d _".

=head2 flags

Read/write.  This is a hashref of parameters and options for how directories
should be scanned and which information is collected.  Each member of 'flags'
has its own accessor method, but they may be accessed here for easy swapping
of entire parameter sets.  All flags are read/write, and most are simple
booleans.

=head2 id_mapper

Read/write.  Scanner collects unix UID and GID if the flag 'include_unix_perm'
is set.  If uid_mapper is non-null, Scanner will also collect the username and
group name.  uid_mapper doesn't need to derive from any particular class; it
just needs methods 'resolve_uid' and 'resolge_gid' which take one argument and
return a string.

The default is an object that uses getpwuid and getgrgid, and caches the
results.

=cut

sub dir_format {
	my $self= shift;
	$self->{dir_format}= $_[1] if (scalar(@_)>1);
	$self->{dir_format}
}

our %_flag_defaults;
BEGIN {
	%_flag_defaults= (
		die_on_dir_error  => 1,
		die_on_file_error => 1,
		die_on_hint_error => 1,
		include_unix_perm => 1,
		include_unix_time => 0,
		include_unix_misc => 0,
		include_acl       => 0,
		include_ext_attr  => 0,
		follow_symlink    => 0,
		cross_mountpoints => 0,
	);
	for (keys %_flag_defaults) {
		eval "sub $_ { \$_[0]{flags}{$_}= \$_[1] if \@_ > 1; \$_[0]{flags}{$_} }; 1" or die $@
	}
	for (qw: filter flags id_mapper :) {
		eval "sub $_ { \$_[0]{$_}= \$_[1] if \@_ > 1; \$_[0]{$_} }; 1" or die $@
	}
}

sub _handle_hint_error {
	croak $_[1] if $_[0]->die_on_hint_error;
	warn "$_[1]\n";
}

sub _handle_file_error {
	croak $_[1] if $_[0]->die_on_file_error;
	warn "$_[1]\n";
}

sub _handle_dir_error {
	croak $_[1] if $_[0]->die_on_dir_error;
	warn "$_[1]\n";
}

=head1 METHODS

=head2 new( %params | \%params)

=over

=item dir_format - optional

Allows you to specify the default directory encoding that will be used for
put_dir.  See the dir_class attribute.

The parameter can be a full class name, or a string without colons which
will be interpreted as a package in the DataStore::CAS::FS::Dir:: namespace.

The default dir_format is "universal", which encodes all
metadata using a canonical JSON format.  This isn't particularly efficient
though, and most likely you want the "Unix" encoding (which stores only the
standard "stat" array, without the inefficiency of hash keys)

=back

=cut

sub new {
	my $class= shift;
	my %p= ref($_[0])? %{$_[0]} : @_;
	$class->_ctor(\%p);
}

our @_ctor_params= (qw: dir_format filter flags id_mapper :, keys %_flag_defaults);

sub _ctor {
	my ($class, $p)= @_;
	my %self= map { $_ => delete $p->{$_} } @_ctor_params;
	croak "Invalid param(s): ".join(', ', keys %$p)
		if keys %$p;

	# Extract flags into their own hashref
	my $flags= ($self{flags} ||= {});
	%self= (%_flag_defaults, %self, %$flags);
	@{$flags}{keys %_flag_defaults}= delete @self{keys %_flag_defaults};

	$self{uid_cache} ||= {};
	$self{gid_cache} ||= {};
	$self{dir_format}= 'universal'
		unless defined $self{dir_format};

	exists $self{id_mapper}
		or $self{id_mapper}= DataStore::CAS::FS::Scanner::DefaultIdMapper->new();
	bless \%self, $class;
}

sub _split_dev_node {
	($_[1] >> 8).','.($_[1] & 0xFF);
}

sub _stat {
	my ($self, $path)= @_;
	my @stat= $self->follow_symlink? stat($path) : lstat($path);
	unless (@stat) {
		$self->_handle_dir_error("Can't stat '$path': $!");
		return undef;
	}
	bless \@stat, 'DataStore::CAS::FS::Scanner::FastStat';
}

my %_ModeToType= ( S_IFREG() => 'file', S_IFDIR() => 'dir', S_IFLNK() => 'symlink',
	S_IFBLK() => 'blockdev', S_IFCHR() => 'chardev', S_IFIFO() => 'pipe', S_IFSOCK() => 'socket' );

sub scan_dir_ent {
	my ($self, $path, $prev_ent_hint, $ent_name, $stat)= @_;
	
	$stat ||= $self->_stat($path)
		or return undef;

	defined $ent_name
		or (undef, undef, $ent_name)= splitpath($path);
	
	my %attrs= (
		type => ($_ModeToType{$stat->[2] & S_IFMT}),
		name => $ent_name,
		size => $stat->[7],
		modify_ts => $stat->[9],
	);
	if ($self->include_unix_perm) {
		$attrs{unix_uid}= $stat->[4];
		$attrs{unix_gid}= $stat->[5];
		$attrs{unix_mode}= $stat->[2];
		if (my $m= $self->id_mapper) {
			$attrs{unix_user}= $m->resolve_uid($stat->[4]);
			$attrs{unix_group}= $m->resolve_gid($stat->[5]);
		}
	}
	if ($self->include_unix_time) {
		$attrs{unix_atime}= $stat->[8];
		$attrs{unix_ctime}= $stat->[10];
	}
	if ($self->include_unix_misc) {
		$attrs{unix_dev}= $stat->[0];
		$attrs{unix_inode}= $stat->[1];
		$attrs{unix_nlink}= $stat->[3];
		$attrs{unix_blocksize}= $stat->[11];
		$attrs{unix_blockcount}= $stat->[12];
	}
	if ($self->include_acl) {
		# TODO
	}
	if ($self->include_ext_attr) {
		# TODO
	}
	if ($attrs{type} eq 'dir') {
		delete $attrs{size};
	}
	elsif ($attrs{type} eq 'file') {
		if ($prev_ent_hint) {
			$attrs{hash}= $prev_ent_hint->hash
				if $prev_ent_hint->type eq 'file'
					and length $prev_ent_hint->hash
					and defined $prev_ent_hint->size
					and defined $prev_ent_hint->modify_ts
					and $prev_ent_hint->size eq $attrs{size}
					and $prev_ent_hint->modify_ts eq $attrs{modify_ts};
		}
	}
	elsif ($attrs{type} eq 'symlink') {
		$attrs{ref}= readlink $path;
	}
	elsif ($attrs{type} eq 'blockdev' or $attrs{type} eq 'chardev') {
		$attrs{ref}= $self->_split_dev_node($stat->[6]);
	}
	\%attrs;
}

sub scan_dir {
	my ($self, $path, $dir_hint)= @_;
	my $dh;
	my @entries;
	my $filter= $self->filter;
	if (!opendir($dh, $path)) {
		$self->_handle_dir_error("Can't open '$path': $!");
		return undef;
	}
	while (defined(my $ent_name= readdir($dh))) {
		next if $ent_name eq '.' or $ent_name eq '..';

		my $ent_path= catfile($path, $ent_name);
		my $stat= $self->_stat($ent_path);

		next if $filter && !$filter->($ent_name, $ent_path, $stat);

		my $dir_ent_hint= ($dir_hint && ($stat->mode & S_IFREG))? $dir_hint->get_entry($ent_name) : undef;
		push @entries, $self->scan_dir_ent($ent_path, $dir_ent_hint, $ent_name, $stat);
	}
	closedir $dh;
	
	return { metadata => {}, entries => [ sort { $a->{name} cmp $b->{name} } @entries ] };
}

sub store_dir {
	my ($self, $cas, $dir_path, $dir_hint)= @_;

	my $dir_stat= $self->_stat($dir_path);
	my $data= $self->scan_dir($dir_path, $dir_hint)
		or return ''; # We've already emitted a warning, if it didn't die.

	# That gave us an array of metadata for the entries of the directory.
	# Now we walk through it filling in the missing 'hash' fields.
	for my $entry (@{$data->{entries}}) {
		# The hash could have been borrowed from the $dir_hint
		next if defined $entry->{hash};

		if ($entry->{type} eq 'file') {
			my $fname= catfile($dir_path, $entry->{name});
			if (open my $fh, '<:raw', $fname) {
				$entry->{hash}= $cas->put_handle($fh);
			} else {
				$self->_handle_file_error("Can't open '$fname': $!");
				# if we didn't die in that method, it means the user wants to proceeed
				# even if a file can't be read.  We set 'hash' to an empty string.
				$entry->{hash}= '';
			}
		}
		elsif ($entry->{type} eq 'dir') {
			my $dname= catdir($dir_path, $entry->{name});
			next
				unless $self->cross_mountpoints
				or ($dir_stat->dev == $self->_stat($dname)->dev);

			my ($subdir_hint, $subdir_hint_ent, $subdir_hint_file);
			if ($dir_hint
				and ($subdir_hint_ent= $dir_hint->get_entry($entry->{name}))
				and ($subdir_hint_ent->type eq 'dir')
				and (defined $subdir_hint_ent->hash)
				and (length $subdir_hint_ent->hash)
			) {
				($subdir_hint_file= $cas->get($subdir_hint_ent->hash))
					and ($subdir_hint= try { DataStore::CAS::FS::DirCodec->load($subdir_hint_file); })
					or $self->_handle_hint_error(
						"Unable to load hint dir for '$dname'"
						." (hash ".$subdir_hint_ent->hash.")"
					);
			}
			$entry->{hash}= $self->store_dir($cas, $dname, $subdir_hint, $self->dir_format);
		}
	}
	# now, we encode it!
	return DataStore::CAS::FS::DirCodec->store($cas, $self->dir_format, $data->{entries}, $data->{metadata} );
}

package DataStore::CAS::FS::Scanner::FastStat;
use strict;
use warnings;

=head1 STAT OBJECTS

The stat arrayrefs that Scanner passes to the filter are blessed to give you
access to methods like '->mode' and '->mtime', but I'm not using File::stat.
"Why??" you ask? because blessing an arrayref from the regular stat is 3 times
as fast and my accessors are twice as fast, and it requires a miniscule amount
of code.

=cut

sub dev     { $_[0][0] }
sub ino     { $_[0][1] }
sub mode    { $_[0][2] }
sub nlink   { $_[0][3] }
sub uid     { $_[0][4] }
sub gid     { $_[0][5] }
sub rdev    { $_[0][6] }
sub size    { $_[0][7] }
sub atime   { $_[0][8] }
sub mtime   { $_[0][9] }
sub ctime   { $_[0][10] }
sub blksize { $_[0][11] }
sub blocks  { $_[0][12] }

package DataStore::CAS::FS::Scanner::DefaultIdMapper;
use strict;
use warnings;

sub new {
	bless { uid_cache => {}, gid_cache => {} }, $_[0];
}

sub resolve_uid {
	$_[0]{uid_cache}{$_[1]} ||= getpwuid($_[1]);
}

sub resolve_gid {
	$_[0]{gid_cache}{$_[1]} ||= getgrgid($_[1]);
}

1;