package File::CAS::Extractor;
use strict;
use warnings;

use Unix::Mknod;
use Fcntl ':mode';
use POSIX;

sub extract {
	my ($self, $cas, $entry, $destPath)= @_;
	my $name= catfile($destPath,$entry->name);
	my $hash= $entry->hash;
	if (($entry->type eq 'file' or $entry->type eq 'dir') and (!defined $hash or !length $hash)) {
		warn "Skipping stub '$name'\n";
		return;
	}
	
	if ($entry->type eq 'file') {
		my $f= $cas->get($hash)
			or die "Store is missing entry '$hash'\n";
		open my $fh, '>:raw', $fnam
			or die "open: $!";
		my $buf;
		while (my $n= $f->read($buf, 64*1024)) {
			print $fh $buf;
		}
		close $fh or die "close: $!";
	}
	elsif ($entry->type eq 'dir') {
		my $d= $cas->getDir($hash)
			or die "Store is missing entry '$hash'\n";
		for ($d->getEntries) {
			$self->extract($cas, $_, $name);
		}
	}
	elsif ($entry->type eq 'symlink') {
		symlink $entry->symlinkTarget, $name
			or die "Failed to create symbolic link '$name': $!\n";
	}
	elsif ($entry->type eq 'blockdev' or $entry->type eq 'chardev') {
		my ($major, $minor)= split /,/, $entry->device;
		defined $major and defined $minor or die "Invalid device for $name: ".$entry->device."\n";
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

1;