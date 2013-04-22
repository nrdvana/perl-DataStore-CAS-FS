package App::Casbak::Cmd::Import;
use Moo;
extends 'App::Casbak::Cmd';
use Try::Tiny;

sub short_description {
	"Import files from filesystem to virtual path in backup"
}

# Array of source=>dest pairs to process
has paths => ( is => 'rw', default => sub { [] } );
sub path_list { @{$_[0]->paths} }

# Flag for whether to only use filesystem metadata (size, mtime) instead of re-hashing all the files
has compare_meta_only => ( is => 'rw' );

# Set of arbitrary metadata to attach to snapshot
has snapshot_meta     => ( is => 'rw', default => sub { {} } );

sub apply_args {
	my ($self, @args)= @_;
	
	require Getopt::Long;
	Getopt::Long::Configure(qw: no_ignore_case bundling permute :);
	Getopt::Long::GetOptionsFromArray(\@args,
		$self->_base_getopt_config,
		'quick'       => sub { $self->compare_meta_only(1) },
		'<>'          => sub { $self->add_path("$_[0]") },
		'as=s'        => sub { $self->set_virtual_path("$_[1]") },
		'comment|m=s' => sub { $self->snapshot_meta->{comment}= "$_[1]" },
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
	
	unless (scalar $self->path_list) {
		my $msg= "No paths specified.  Nothing to do\n";
		$self->allow_no_op or die $msg;
		warn $msg;
		return 1;
	}
	
	# Create instance of Casbak
	my $casbak= App::Casbak->new($self->casbak_args);
	
	# Start from current snapshot.
	# (It could be undef, which means we're starting from scratch)
	my $snap= $casbak->get_snapshot();
	my $fs= DataStore::CAS::FS->new(
		store => $casbak->cas,
		root_entry => $snap? $snap->root_entry : {}
	);

	for my $path_spec ($self->path_list) {
		my $hint;
		if ($self->compare_meta_only) {
			my $hint_path= $fs->resolve_path($path_spec->{virt}, { no_die => 1 });
			if (defined $hint_path && @$hint_path > 0 && $hint_path->[-1]->type eq 'dir') {
				my $digest_hash= $hint_path->[-1]->ref;
				$hint= $fs->get_dir($digest_hash)
					if defined $digest_hash and length $digest_hash;
			}
		}
		my $digest_hash= $casbak->scanner->store_dir($casbak->cas, $path_spec->{real}, $hint);
		my $new_ent= $casbak->scanner->scan_dir_ent($path_spec->{real});
		delete $new_ent->{name};
		$new_ent->{ref}= $digest_hash;
		$fs->set_path($path_spec->{virt}, $new_ent, { force_create => 1 });
	}
	p($fs->_path_overrides);

	# TODO: Fill in interesting metadata about this backup (duration, num files, size increase, etc)
	
	# Save this new root as a snapshot
	$fs->commit();
	$casbak->save_snapshot($fs->root_entry, $self->snapshot_meta);
	1;
}

sub get_pod {
	open(my $f, '<', __FILE__)
		or die "Unable to read script (".__FILE__.") to extract help text: $!\n";
	local $/= undef;
	return scalar <$f>;
}

1;

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
