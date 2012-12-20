package App::Casbak::Cmd::Import;
use strict;
use warnings;
use Try::Tiny;

use parent 'App::Casbak::Cmd';

sub ShortDescription {
	"Import files from filesystem to virtual path in backup"
}

sub paths { $_[0]{paths} }
sub path_list { @{$_[0]->paths} }
sub compareMetaOnly { $_[0]{compareMetaOnly} }

sub _ctor {
	my ($class, $params)= @_;
	$params->{paths} ||= [];
	$class->SUPER::_ctor($params);
}

sub applyArguments {
	my ($self, @args)= @_;
	
	require Getopt::Long;
	Getopt::Long::Configure(qw: no_ignore_case bundling permute :);
	Getopt::Long::GetOptionsFromArray(\@args,
		$self->_baseGetoptConfig,
		'quick'   => \$self->{compareMetaOnly},
		'<>'      => sub { $self->add_path("$_[1]") },
		'as=s'    => sub { $self->set_virtual_path("$_[1]") },
		) or die "\n";
	
}

sub add_path {
	my ($self, $path)= @_;
	push @{$self->paths}, { real => $path, virt => $path };
}

sub set_virtual_path {
	my ($self, $virt)= @_;
	die "No preceeding path for '--as'\n"
		unless scalar @{$self->paths};
	$self->paths->[-1]{virt}= $virt;
}

sub run {
	my $self= shift;
	
	unless scalar($self->path_list) {
		my $msg= "No paths specified.  Nothing to do\n";
		$self->allowNoop or die $msg;
		warn $msg;
		return 1;
	}
	
	# Create instance of Casbak
	my $casbak= App::Casbak->new($self->casbakConfig);
	
	# Start from current snapshot.
	# Note that $root is a checksum string and not an object, here.
	# It will get inflated to an object during 'importTree'
	my $snap= $casbak->getSnapshot();

	for my $pathSpec ($self->path_list) {
		$root= $casbak->importTree(snapshot => $snap, %$pathSpec, compareMetaOnly => $self->compareMetaOnly);
	}
	
	# 
	$casbak->saveSnapshot($root);
}

__END__

=head1 NAME

casbak-import - import files from real filesystem to virtual CAS filesystem

=head1 SYNOPSIS

  casbak-import [options] PATH [--as VIRTUAL_PATH] ...

=head1 OPTIONS

=over 20

=item --as VIRTUAL_PATH

Files are backed up from PATH (canonical, fully resolved, no symlinks)
in the real filesystem to a virtual path in the backup by the same
(canonical) name.

If you want the backup to contain the files in a different layout than
the real filesystem, you may specify "--as VIRTUAL_PATH" after a target.

Note that (at this time) VIRTUAL_PATH may not contain symlinks.  This support
will be added in the future.

=item --quick

Skip the checksum calculation for a file if its timestamp
and size have not changed from the previous run.  This
requires a directory encoding that preserves mtime, and for
a previous backup of those files to exist at the specified
virtual path.  If "quick" mode is not available, it will
calculate checksums as normal (and warn you if "verbose"
is enabled).

=back

See "casbak --help" for general-purpose options.

=head1 EXAMPLES

  # Backup the directories /usr/bin, /usr/local/bin, and /bin
  # storing each in the same-named location of the backup's hierarchy
  casbak-import /usr/bin /usr/local/bin /bin

  # Backup the directory /tmp/new_bin as /bin in the backup's hierarchy
  casbak-import /tmp/new_bin --as /bin

  # Backup several directories, but hide the partition boundary
  #  which shows up after casbak resolves the symlinks.
  # Example Symlinks:
  #   /var -> /mnt/var
  #   /usr -> /mnt/sys/usr
  casbak-import /bin /sbin /etc /usr --as /usr /var --as /var

=head1 SECURITY

See discussion in "casbak --help"

=cut
