package File::CAS::DirScan;

use 5.006;
use strict;
use warnings;

use Carp;
use Fcntl ':mode';
use File::Spec::Functions 'catfile', 'catdir';
use File::CAS::DirEntry;

sub dieOnDirError   { $_[0]{dieOnDirError} }
sub dieOnFileError  { $_[0]{dieOnFileError} }
sub dieOnHintError  { $_[0]{dieOnHintError} }
sub includeUnixPerm { $_[0]{includeUnixPerm} }
sub includeUnixTime { $_[0]{includeUnixTime} }
sub includeUnixMisc { $_[0]{includeUnixMisc} }
sub includeACL      { $_[0]{includeACL} }
sub includeExtAttr  { $_[0]{includeExtAttr} }
sub followSymlink   { $_[0]{followSymlink} }

sub new {
	my $class= shift;
	my %p= ref($_[0])? %{$_[0]} : @_;
	$class->_ctor(\%p);
}

sub _ctor {
	my ($class, $p)= @_;
	my %self= (
		dieOnDirError   => (defined $p->{dieOnDirError}   ? delete $p->{dieOnDirError} : 1),
		dieOnFileError  => (defined $p->{dieOnFileError}  ? delete $p->{dieOnFileError} : 1),
		dieOnHintError  => (defined $p->{dieOnHintError}  ? delete $p->{dieOnHintError} : 1),
		includeUnixPerm => (defined $p->{includeUnixPerm} ? delete $p->{includeUnixPerm} : 1),
		includeUnixTime => (defined $p->{includeUnixTime} ? delete $p->{includeUnixTime} : 0),
		includeUnixMisc => (defined $p->{includeUnixMisc} ? delete $p->{includeUnixMisc} : 0),
		includeACL      => (defined $p->{includeACL}      ? delete $p->{includeACL} : 0),
		includeExtAttr  => (defined $p->{includeExtAttr}  ? delete $p->{includeExtAttr} : 0),
		followSymlink   => (defined $p->{followSymlink}   ? delete $p->{followSymlink} : 0),
	);
	croak "Invalid param(s): ".join(', ', keys %$p)
		if keys %$p;
	bless \%self, $class;
}

sub _splitDevNode {
	($_[1] >> 8).','.($_[1] & 0xFF);
}

my %_ModeToType= ( S_IFREG() => 'file', S_IFDIR() => 'dir', S_IFLNK() => 'symlink',
	S_IFBLK() => 'blockdev', S_IFCHR() => 'chardev', S_IFIFO() => 'pipe', S_IFSOCK() => 'socket' );

sub scan {
	my ($self, $dir, $dirHint, $filter)= @_;
	
	my $dh;
	my @entries;
	if (!opendir($dh, $dir)) {
		my $msg= "Can't open '$dir': $!";
		croak $msg
			if $self->dieOnDirError;
		warn $msg."\n";
		return '';
	}
	while (defined(my $entName= readdir($dh))) {
		my $path= catfile($dir, $entName);
		my @stat= $self->followSymlink? stat($path) : lstat($path);
		if (!scalar @stat) {
			my $msg= "Can't stat '$path': $!";
			croak $msg
				if $self->dieOnDirError;
			warn $msg."\n";
			next;
		}
		my $ent;
		next if $filter && !$filter->($ent, \@stat);
		my %args= (
			type => ($_ModeToType{$stat[2] & S_IFMT}),
			name => $entName,
			size => $stat[7],
			modify_ts => $stat[11],
		);
		if ($self->includeUnixPerm) {
			$args{uid}= $stat[4];
			$args{gid}= $stat[5];
			$args{mode}= ($stat[2] & ~S_IFMT);
		}
		if ($self->includeUnixTime) {
			$args{atime}= $stat[10];
			$args{ctime}= $stat[12];
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
			if ($dirHint && ($prevEnt= $dirHint->entryHash->{$entName})) {
				$args{hash}= $prevEnt->hash
					if $prevEnt->type eq $args{type}
						and $prevEnt->size eq $args{size}
						and $prevEnt->modify_ts eq $args{modify_ts};
			}
		}
		elsif ($args{type} eq 'symlink') {
			$args{linkTarget}= readlink $path;
		}
		elsif ($args{type} eq 'blkdev' or $args{type} eq 'chrdev') {
			$args{device}= $self->_splitDevNode($stat[6]);
		}
		push @entries, \%args;
	}
	closedir $dh;
	
	return { metadata => {}, entries => [ sort { $a->{name} cmp $b->{name} } @entries ] };
}

1;