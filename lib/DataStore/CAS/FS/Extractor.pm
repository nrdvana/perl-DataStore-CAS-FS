package DataStore::CAS::FS::Extractor;
use 5.008;
use Moo;
use Carp;

use Unix::Mknod;
use Fcntl ':mode';
use POSIX;

sub extract {
	my ($self, $cas, $entry, $dest_path)= @_;
	my $name= catfile($dest_path, $entry->name);
	my $hash= $entry->ref;
	if (($entry->type eq 'file' or $entry->type eq 'dir') and (!defined $hash or !length $hash)) {
		warn "Skipping stub '$name'\n";
		return;
	}
	
	if ($entry->type eq 'file') {
		my $f= $cas->get($hash)
			or die "Store is missing entry '$hash'\n";
		open(my $dest_fh, '>', $name) or die "open($name): $!\n";
		my $src_fh= $f->open;
		$self->_copy_fh($src_fh, $dest_fh);
		close $src_fh or die "close: $!";
		close $dest_fh or die "close: $!";
	}
	elsif ($entry->type eq 'dir') {
		my $d= DataStore::CAS::FS::DirCodec->load($cas->get($hash))
			or die "Store is missing entry '$hash'\n";
		my $iter= $d->iterator;
		while (defined (my $ent= $iter->())) {
			$self->extract($cas, $ent, $name);
		}
	}
	elsif ($entry->type eq 'symlink') {
		symlink $entry->ref, $name
			or die "Failed to create symbolic link '$name': $!\n";
	}
	elsif ($entry->type eq 'blockdev' or $entry->type eq 'chardev') {
		my ($major, $minor)= split /,/, $entry->ref;
		defined $major and defined $minor or die "Invalid device for $name: ".$entry->ref."\n";
		my $mode= $entry->unix_mode;
		$mode= ($entry->type eq 'blockdev')? (S_IFBLK | 0660) : (S_IFCHR | 0660)
			unless (defined $mode);
		Unix::Mknod::mknod($name, $mode, Unix::Mknod::makedev($major, $minor))
			or die "Failed to create dev node '$name': $!\n";
	}
	elsif ($entry->type eq 'pipe') {
		my $mode= $entry->unix_mode;
		$mode= 0660 unless $mode;
		POSIX::mkfifo($name, $mode)
			or die "Failed to create pipe '$name': $!\n";
	}
	#elsif ($entry->type eq 'socket') {
	# TODO: but is there really any point?	
	#}
	else {
		warn "Skipping '$name' of type '".$entry->type."'\n";
	}
}

sub _copy_fh {
	my ($src, $dest)= @_;
	my ($buf, $got);
	while ($got= read($src, $buf, 1024*1024)) {
		(print $dest, $buf) or die "write: $!\n";
	}
	defined $got or die "read: $!\n";
}

1;