package File::CAS::Scanner;

use 5.006;
use strict;
use warnings;

use Carp;
use Fcntl ':mode';
use Try::Tiny;
use File::Spec::Functions 'catfile', 'catdir';

=head2 dirClass - read/write

Directories can be recorded with varying levels of metadata
(determined by the scanner) and encoded in a variety of formats which
are optimized for various uses.

You must set this to a full package name.  The partial names are only
allowed in the constructor.

This class will be the default used by 'storeDir'.

=cut

sub dieOnDirError   { $_[0]{dieOnDirError} }
sub dieOnFileError  { $_[0]{dieOnFileError} }
sub dieOnHintError  { $_[0]{dieOnHintError} }
sub includeUnixPerm { $_[0]{includeUnixPerm} }
sub includeUnixTime { $_[0]{includeUnixTime} }
sub includeUnixMisc { $_[0]{includeUnixMisc} }
sub includeACL      { $_[0]{includeACL} }
sub includeExtAttr  { $_[0]{includeExtAttr} }
sub followSymlink   { $_[0]{followSymlink} }

sub filter          { $_[0]{filter} }

sub dirClass        { $_[0]{dirClass}= $_[1] if (scalar(@_)>1); $_[0]{dirClass} || 'File::CAS::Dir' }

sub _handleHintError {
	croak $_[1] if $_[0]->dieOnHintError;
	warn "$_[1]\n";
}

sub _handleFileError {
	croak $_[1] if $_[0]->dieOnFileError;
	warn "$_[1]\n";
}

sub _handleDirError {
	croak $_[1] if $_[0]->dieOnDirError;
	warn "$_[1]\n";
}


=head2 new( %params | \%params)

=over

=item dirClass - optional

Allows you to specify the default directory encoding that will be used for
putDir.  See the dirClass attribute.

The parameter can be a full class name, or a string without colons which
will be interpreted as a package in the File::CAS::Dir:: namespace.

The default dirClass is "File::CAS::Dir", which encodes all metadata using
a canonical JSON format.  This isn't particularly efficient though, and
most likely you want the "Unix" encoding (which stores only the standard
"lstat" array, without the inefficiency of hash keys)

=back

=cut

sub new {
	my $class= shift;
	my %p= ref($_[0])? %{$_[0]} : @_;
	$class->_ctor(\%p);
}

our %_ctor_defaults= (
	dieOnDirError   => 1,
	dieOnFileError  => 1,
	dieOnHintError  => 1,
	includeUnixPerm => 1,
	includeUnixTime => 0,
	includeUnixMisc => 0,
	includeACL      => 0,
	includeExtAttr  => 0,
	followSymlink   => 0,
);
	
our @_ctor_params= qw: dirClass :, keys %_ctor_defaults;

sub _ctor {
	my ($class, $p)= @_;
	my %self= map { $_ => delete $p->{$_} } @_ctor_params;
	croak "Invalid param(s): ".join(', ', keys %$p)
		if keys %$p;
	$self{$_}= $_ctor_defaults{$_} for keys %_ctor_defaults;
	bless \%self, $class;
}

sub _splitDevNode {
	($_[1] >> 8).','.($_[1] & 0xFF);
}

my %_ModeToType= ( S_IFREG() => 'file', S_IFDIR() => 'dir', S_IFLNK() => 'symlink',
	S_IFBLK() => 'blockdev', S_IFCHR() => 'chardev', S_IFIFO() => 'pipe', S_IFSOCK() => 'socket' );

sub scanDir {
	my ($self, $path, $dirHint)= @_;
	
	my $dh;
	my @entries;
	my $filter= $self->filter;
	if (!opendir($dh, $path)) {
		$self->_handleDirError("Can't open '$path': $!");
		return undef;
	}
	while (defined(my $entName= readdir($dh))) {
		next if $entName eq '.' or $entName eq '..';
		
		my $entPath= catfile($path, $entName);
		
		my @stat= $self->followSymlink? stat($entPath) : lstat($entPath);
		unless (@stat) {
			$self->_handleDirError("Can't stat '$path': $!");
			next;
		}
		
		next if $filter && !$filter->($entName, $entPath, \@stat);
		
		my %args= (
			type => ($_ModeToType{$stat[2] & S_IFMT}),
			name => $entName,
			size => $stat[7],
			modify_ts => $stat[11],
		);
		if ($self->includeUnixPerm) {
			$args{unix_uid}= $stat[4];
			$args{unix_gid}= $stat[5];
			$args{unix_mode}= $stat[2];
		}
		if ($self->includeUnixTime) {
			$args{unix_atime}= $stat[10];
			$args{unix_ctime}= $stat[12];
		}
		if ($self->includeUnixMisc) {
			$args{unix_dev}= $stat[0];
			$args{unix_inode}= $stat[1];
			$args{unix_nlink}= $stat[3];
			$args{unix_blocksize}= $stat[8];
			$args{unix_blocks}= $stat[9];
		}
		if ($self->includeACL) {
			# TODO
		}
		if ($self->includeExtAttr) {
			# TODO
		}
		if ($args{type} eq 'dir') {
			$args{size}= 0;
		}
		elsif ($args{type} eq 'file') {
			my $prevEnt;
			use Data::Dumper;
			if ($dirHint && ($prevEnt= $dirHint->getEntry($entName))) {
				$args{hash}= $prevEnt->hash
					if $prevEnt->type eq 'file'
						and length $prevEnt->hash
						and defined $prevEnt->size
						and defined $prevEnt->modify_ts
						and $prevEnt->size eq $args{size}
						and $prevEnt->modify_ts eq $args{modify_ts};
			}
		}
		elsif ($args{type} eq 'symlink') {
			$args{linkTarget}= readlink $path;
		}
		elsif ($args{type} eq 'blockdev' or $args{type} eq 'chardev') {
			$args{device}= $self->_splitDevNode($stat[6]);
			$args{unix_dev}= $stat[6];
		}
		push @entries, \%args;
	}
	closedir $dh;
	
	return { metadata => {}, entries => [ sort { $a->{name} cmp $b->{name} } @entries ] };
}

sub storeDir {
	my ($self, $cas, $dirPath, $dirHint, $dirClass)= @_;
	$dirClass ||= $self->dirClass;
	my $data= $self->scanDir($dirPath, $dirHint);
	if ($data) {
		for my $entry (@{$data->{entries}}) {
			next if defined $entry->{hash};
			if ($entry->{type} eq 'file') {
				my $fname= catfile($dirPath,$entry->{name});
				if (open my $fh, "<:raw", $fname) {
					$entry->{hash}= $cas->putHandle($fh);
				} else {
					$self->_handleFileError("Can't open '$fname': $!");
					# if the user really wants us to ignore files we can't read, we set 'hash' to an empty string.
					$entry->{hash}= '';
				}
			}
			elsif ($entry->{type} eq 'dir') {
				my $dname= catdir($dirPath, $entry->{name});
				my $subHint;
				if ($dirHint) {
					$subHint= try { $dirHint->subdir($entry->{name}) } catch { \"$_" };
					if (ref $subHint eq 'SCALAR') {
						$self->_handleHintError("Error loading hint for '$dname'");
						$subHint= undef;
					}
				}
				$entry->{hash}= $self->storeDir($cas, $dname, $subHint, $dirClass);
			}
		}
		# now, we encode it!
		my $serialized= $dirClass->SerializeEntries( $data->{entries}, $data->{metadata} );
		return $cas->putScalar($serialized);
	}
	else {
		# We've already emitted a warning, if it didn't die.
		# We return an empty hash value to indicate unknown content.
		return '';
	}
}

1;