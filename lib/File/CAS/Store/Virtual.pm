package File::CAS::Store::Virtual;
use strict;
use warnings;

use Carp;
use File::CAS::File;
use Digest;

sub new {
	my $class= shift;
	$class->_ctor({ ref($_[1])? %{$_[1]} : @_ });
}

sub _ctor {
	my ($class, $params)= @_;
	$params ||= {};
	$params->{alg}= 'MD5' unless $params->{alg} && $params->{alg} ne 'auto';
	bless $params, $class;
}

sub entries { $_[0]{entries} ||= {} }
sub alg { $_[0]{alg} }

sub get {
	my ($self, $hash)= @_;
	return undef unless defined $self->entries->{$hash};
	return File::CAS::File->_ctor({ store => $self, hash => $hash, size => length($self->entries->{$hash}) });
}

sub put {
	my ($self, $data, $hash)= @_;
	if (ref $data) {
		local $/= undef;
		$data= <$data>;
	}
	$hash ||= $self->calcHash($data);
	$self->entries->{$hash}= $data;
	$hash;
}

sub calcHash {
	my ($self, $data)= @_;
	Digest->new($self->alg)->add($data)->hexdigest;
}

sub validate {
	my ($self, $hash)= @_;
	
	(defined $hash && !defined($self->entries->{$hash}))
		and croak "No such entry: '$hash'";
	
	my @invalid;
	for (defined $hash? ($hash) : (keys %{$self->entries})) {
		push @invalid, $_ unless $self->calcHash($self->entries->{$_}) eq $_;
	}
	return 1 unless @invalid;
	return wantarray? (0, \@invalid) : 0;
}

sub readFile {
	my ($self, $file, undef, $length, $offset)= @_;
	my $src= $self->entries->{$file->hash};
	$file->{_pos} ||= 0;
	my $count= $length;
	my $remain= length($src) - $file->{_pos};
	$count= $remain if ($remain < $count);
	$_[2]= '' unless defined $_[2];
	substr($_[2], $offset||0)= substr($src, $file->{_pos}, $count);
	$file->{_pos} += $count;
	$count;
}

sub seekFile {
	my ($self, $file, $ofs, $whence)= @_;
	my $src= $self->entries->{$file->hash};
	$file->{_pos} ||= 0;
	$ofs ||= 0;
	$whence ||= 0;
	$ofs += $file->{_pos} if $whence == 1;
	$ofs += length($src) if $whence == 2;
	$ofs= length($src) if $ofs > length($src);
	$ofs= 0 if $ofs < 0;
	($file->{_pos}= $ofs) || '0 but true';
}

sub closeFile {
	my ($self, $file)= @_;
	delete $file->{_pos};
}

1;