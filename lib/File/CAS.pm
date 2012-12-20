package File::CAS;

use 5.006;
use strict;
use warnings;

=head1 NAME

File::CAS - Content-Addressable Storage for file trees

=head1 DESCRIPTION

TODO:

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.0100';
sub VersionParts {
	return (int($VERSION), (int($VERSION*100)%100), (int($VERSION*10000)%100));
}

use Carp;
use File::Spec;
use File::CAS::File;
use File::CAS::Dir;
use File::CAS::Scanner;

=head1 SYNOPSIS

File::CAS is an object that implements Content Addressable Storage that behaves
like a file/directory filesystem.

Content Addressable Storage is a concept where a file is identified by a hash of
its content, and you can only retrieve it if you know the hash you are looking
for.  File::CAS extends this to also include a directory hierarchy to let you
look up the file you are interested in by a path name.

File::CAS is mostly a wrapper around pluggable modules that handle the details.
The primary object involved is a File::CAS::Store, which performs the hashing
and storage actions.  There is also File::CAS::Scanner for scanning the real
filesystem to import directories, and various directory encoding classes like
File::CAS::Dir::Unix used to serialize and deserialize the directories.

=head1 ATTRIBUTES

=head2 store - read-only

An instance of 'File::CAS::Store' or a subclass.

=head2 scanner - read/write

An instance of File::CAS::Scanner, or subclass.  It is responsible for
scanning real filesystem directories during "putDir".  If you didn't
specify one in the constructor, one will be created automatically.

You may alter this instance, or specify an alternate one during the
constructor, or overwrite this attribute with a new object reference.

=cut

sub store { $_[0]{store} }

sub scanner { $_[0]{scanner}= $_[1] if (scalar(@_)>1); $_[0]{scanner} }

=head1 METHODS

=head2 new( %args | \%args )

Parameters:

=over

=item store - required

It may be a class name like 'Simple' which
refers to the namespace File::CAS::Store::, or it may be a fully constructed
instance.  If it is a class name, you may also specify parameters for that
class in-line with the rest of the CAS parameters, and they will be sorted
out automagically.

=item scanner - optional

Allows you to specify a scanner object
which is used during "putDir" to collect metadata about the directory
entries.

=item filter - optional

Alias for scanner->filter.

=back

=cut

sub new {
	my $class= shift;
	my %p= ref($_[0])? %{$_[0]} : @_;
	
	defined $p{store} or croak "Missing required parameter 'store'";
	
	# coercion of store parameters to Store object
	if ($p{store} && !ref $p{store}) {
		$p{store}= { CLASS => $p{store} };
	}
	if (ref $p{store} eq 'HASH') {
		my %storeParams= %{$p{store}};
		my $storeClass= delete $storeParams{CLASS} || 'File::CAS::Store::Simple';
		_requireClass($storeClass);
		$storeClass->isa('File::CAS::Store')
			or die "'$storeClass' is not a valid Store class\n";
		$p{store}= $storeClass->new(\%storeParams);
	}
	
	# coercion of scanner parameters to Scanner object
	$p{scanner} ||= { };
	if (ref $p{scanner} eq 'HASH') {
		my %scannerParams= %{$p{scanner}};
		my $scannerClass= delete $scannerParams{CLASS} || 'File::CAS::Scanner';
		_requireClass($scannerClass);
		$scannerClass->isa('File::CAS::Scanner')
			or die "'$scannerClass' is not a valid Scanner class\n";
		$p{scanner}= $scannerClass->new(\%scannerParams);
	}
	
	$class->_ctor(\%p);
}

sub _requireClass($) {
	my $pkg= shift;
	
	# We're loading user-supplied class names.  Protect against code injection.
	($pkg =~ /^[A-Za-z0-9_:]+$/)
		or croak "Invalid perl package name: '$pkg'\n";
	
	unless ($pkg->can('new')) {
		my ($fail, $err)= do {
			local $@;
			((not eval "require $pkg;"), $@);
		};
		die $err if $fail;
	}
	1;
}

sub getConfig {
	my $self= shift;
	return {
		CLASS => ref $self,
		VERSION => $VERSION,
		store => $self->store->getConfig,
		scanner => $self->scanner->getConfig,
	};
}

our @_ctor_params= qw: scanner store dirClass :;
sub _ctor_params { @_ctor_params }

sub _ctor {
	my ($class, $params)= @_;
	my $p= { map { $_ => delete $params->{$_} } @_ctor_params };
	croak "Invalid parameter: ".join(', ', keys %$params)
		if (keys %$params);
	defined $p->{store} and $p->{store}->isa('File::CAS::Store')
		or croak "Missing/invalid parameter 'store'";
	defined $p->{scanner} and $p->{scanner}->isa('File::CAS::Scanner')
		or croak "Missing/invalid parameter 'scanner'";
	$p->{_dircache}= {};
	$p->{_dircache_recent}= [];
	$p->{_dircache_recent_idx}= 0;
	$p->{_dircache_recent_mask}= 63;
	bless $p, $class;
}

sub get {
	# my ($self, $hash)= @_;
	$_[0]{store}->get($_[1]);
}

sub getDir {
	my ($self, $hash)= @_;
	my $dircache= $self->{_dircache};
	my $ret= $dircache->{$hash};
	unless (defined $ret) {
		$ret= File::CAS::Dir->new($_[0]{store}->get($_[1]));
		if ($ret) {
			Scalar::Util::weaken($dircache->{$hash}= $ret);
			# We don't want a bunch of dead keys laying around in our cache, so we use a clever trick
			# of attaching an object to the directory whose destructor removes the key from our cache.
			# We use a blessed coderef to prevent seeing circular references while debugging.
			$ret->{_dircache_cleanup}= bless sub { delete $dircache->{$hash} }, 'File::CAS::DircacheCleanup';
		}
	}
	# Hold a reference to any dir requested in the last N getDir calls.
	$self->{_dircache_recent}[$self->{_dircache_recent_idx}++ & $self->{_dircache_recent_mask}]= $ret;
	$ret;
}

sub getEmptyDirHash {
	my $self= shift;
	return $self->{emptyDirHash} ||=
		do {
			my $emptyDir= File::CAS::Dir->SerializeEntries([],{});
			$self->{store}->put($emptyDir);
		};
}

sub getEmptyFileHash {
	my $self= shift;
	return $self->{store}->hashOfNull;
}

sub _clearDirCache {
	my ($self)= @_;
	$self->{_dircache}= {};
	$self->{_dircache_recent}= [];
	$self->{_dircache_recent_idx}= 0;
}

sub calcHash {
	my ($self, $thing)= @_;
	if (ref($thing)) {
		if (ref($thing)->isa('Path::Class::Dir')) {
			return $self->dirScanner->calcHash($self, $thing);
		} elsif (ref($thing)->isa('Path::Class::File')) {
			open my $f, '<:raw', "$thing";
			return $self->{store}->calcHash($f);
		}
	}
	$self->{store}->calcHash($thing);
}

sub findHashByPrefix {
	my ($self, $prefix)= @_;
	return $prefix if $self->get($prefix);
	warn "TODO: Implement findHashByPrefix\n";
	return undef;
}

sub put {
	return $_[0]->putScalar($_[1]) unless ref $_[1];
	return $_[0]->putDir($_[1])    if ref($_[1])->isa('Path::Class::Dir');
	return $_[0]->putFile($_[1])   if ref($_[1])->isa('Path::Class::File');
	# else assume handle
	$_[0]{store}->put($_[1]);
}

sub putScalar {
	my ($self, $scalar)= @_;
	$scalar= "$scalar" if ref $scalar;
	$self->{store}->put($scalar);
}

sub putHandle {
	$_[0]{store}->put($_[1]);
}

sub putFile {
	my ($self, $fname)= @_;
	open(my $fh, '<:raw', "$fname")
		or croak "Can't open '$fname': $!";
	$self->{store}->put($fh);
}

sub putDir {
	my ($self, $dir, $dirHint)= @_;
	$self->scanner->storeDir($self, $dir, $dirHint);
}

=head2 resolvePath( $rootDirEnt, \@pathNames | $pathString, [ \$error_out ] )

Returns an arrayref of File::CAS:Dir::Entry objects corresponding to the
specified path, starting with $rootDirEnt.  This function essentially
performs the same operation as 'File::Spec::realpath', but for the virtual
filesystem, and gives you an array of directory entries instead of a string.

If the path contains symlinks or '..', they will be resolved properly.
Symbolic links are not followed if they are the final element of the path.
To force symbolic links to be resolved, simply append '' to @pathNames, or
append '/' to $pathString.

If the path does not exist, or cannot be resolved for some reason, this
method returns undef, and the error is stored in the optional parameter
$error_out.  If you would rather die on an unresolved path, use
'resolvePathOrDie()'.

If you want symbolic links to resolve properly, $rootDirEnt must be the
filesystem root directory. Passing any other directory will cause a chroot-like
effect which you may or may not want.

=head2 resolvePathOrDie

Same as resolvePath, but calls 'croak' with the error message if the
resolve fails.

=cut

sub resolvePath {
	my ($self, $rootDirEnt, $path, $error_out)= @_;
	my $ret= $self->_resolvePath($rootDirEnt, $path);
	return $ret if ref($ret) eq 'ARRAY';
	$$error_out= $ret;
	return undef;
}

sub resolvePathOrDie {
	my ($self, $rootDirEnt, $path)= @_;
	my $ret= $self->_resolvePath($rootDirEnt, $path);
	return $ret if ref($ret) eq 'ARRAY';
	croak $ret;
}

sub _resolvePath {
	my ($self, $rootDirEnt, $path)= @_;
	my @subPath= ref($path)? @$path : File::Spec->splitdir($path);
	my @dirEnts= ( $rootDirEnt );
	die "Root directory must be a directory"
		unless $rootDirEnt->type eq 'dir';
	while (@subPath) {
		my $ent= $dirEnts[-1];
		my $dir;
		
		if ($ent->type eq 'symlink') {
			# Sanity check on symlink entry
			my $target= $ent->linkTarget;
			defined $target and length $target
				or return 'Invalid symbolic link "'.$ent->name.'"';
			
			# Resolve the path in the link before continuing on the remainder of the current path
			unshift @subPath, File::Spec->splitdir($target);
			pop @dirEnts;
			
			# If an absolute link, we start over from the root
			@dirEnts= ( $rootDirEnt )
				if (substr($target, 0, 1) eq '/');
			
			next;
		}
		
		return 'Cannot descend into directory entry "'.$ent->name.'" of type "'.$ent->type.'"'
			unless ($ent->type eq 'dir');
		
		# If no hash listed, directory was not stored. (i.e. --exclude option during import)
		defined $ent->hash
			or return 'Directory "'.$ent->name.'" is not present in storage';
		
		$dir= $self->getDir($ent->{hash});
		defined $dir
			or return 'Failed to open directory "'.$ent->name.'"';
		
		my $name;
		do {
			$name= shift @subPath;
			defined $name
				or next;
		} while (@subPath and (!length $name or $name eq '.'));
		
		if ($name eq '..') {
			die "Cannot access '..' at root directory"
				unless @dirEnts > 1;
			pop @dirEnts;
		}
		else {
			my $ent= $dir->getEntry($name);
			defined $ent
				or return 'No such directory entry "'.$name.'"';
			push @dirEnts, $ent;
		}
	}
	\@dirEnts;
}

=head1 AUTHOR

Michael Conrad, C<< <mike at nrdvana.net> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-file-cas at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=File-CAS>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc File::CAS


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=File-CAS>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/File-CAS>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/File-CAS>

=item * Search CPAN

L<http://search.cpan.org/dist/File-CAS/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2012 Michael Conrad.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

package File::CAS::DircacheCleanup;

sub DESTROY {
	&{$_[0]}; # Our 'object' is actually a blessed coderef that removes us from the cache.
}

1;
