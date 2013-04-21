package DataStore::CAS::FS::DirCodec::Unix;
use 5.008;
use strict;
use warnings;
use Try::Tiny;
use Carp;
use JSON;
use DataStore::CAS::FS::NonUnicode;

use parent 'DataStore::CAS::FS::DirCodec';

our $VERSION= 1.0000;

__PACKAGE__->register_format(unix => __PACKAGE__);

=head1 METHODS

=head2 $class->encode( \@entries, \%metadata )

=cut

our %_TypeToCode= ( file => 'f', dir => 'd', symlink => 'l', chardev => 'c', blockdev => 'b', pipe => 'p', socket => 's' );
our %_CodeToType= map { $_TypeToCode{$_} => $_ } keys %_TypeToCode;
our @fieldOrder= qw( code name value size unix_uid unix_gid unix_mode unix_atime unix_mtime unix_ctime unix_dev unix_inode unix_nlink unix_blocksize unix_blockcount );
sub encode {
	my ($class, $entry_list, $metadata)= @_;
	defined $metadata->{_}
		and croak '$metadata{_} is reserved for the directory encoder';
	my @entries= map { ref $_ eq 'HASH'? DataStore::CAS::FS::DirEnt->new($_) : $_ } @$entry_list;

	# Often, an entire directory will have the same permissions for all entries
	#  or vary only by file/directory type.
	# First we find the default value by whichever appears the most often.
	my %occur;
	defined($_->unix_uid) and $occur{$_->unix_uid}++ for @entries;
	my ($def_uid)= sort { $occur{$a} > $occur{$b} } keys %occur;
	$def_uid= '' unless defined $def_uid;

	%occur= ();
	defined($_->unix_gid) and $occur{$_->unix_gid}++ for @entries;
	my ($def_gid)= sort { $occur{$a} > $occur{$b} } keys %occur;
	$def_gid= '' unless defined $def_gid;

	%occur= ();
	defined($_->unix_mode) and $_->type ne 'dir' and $occur{$_->unix_mode}++ for @entries;
	my ($def_fmode)= sort { $occur{$a} > $occur{$b} } keys %occur;
	$def_fmode= '' unless defined $def_fmode;

	%occur= ();
	defined($_->unix_mode) and $_->type eq 'dir' and $occur{$_->unix_mode}++ for @entries;
	my ($def_dmode)= sort { $occur{$a} > $occur{$b} } keys %occur;
	$def_dmode= '' unless defined $def_dmode;

	%occur= ();
	defined($_->unix_dev) and $occur{$_->unix_dev}++ for @entries;
	my ($def_dev)= sort { $occur{$a} > $occur{$b} } keys %occur;
	$def_dev= '' unless defined $def_dev;
	
	$metadata->{_}{def}= { uid => $def_uid, gid => $def_gid, fmode => $def_fmode, dmode => $def_dmode, dev => $def_dev };

	# Save the mapping of UID to User and GID to Group
	$metadata->{_}{umap}= {
		map { (defined $_->unix_uid && defined $_->unix_user)?
			($_->unix_uid => $_->unix_user) : ()
			} @entries
	};
	$metadata->{_}{gmap}= {
		map { (defined $_->unix_gid && defined $_->unix_group)?
			($_->unix_gid => $_->unix_group) : ()
			} @entries
	};
	
	my $meta_json= JSON->new->utf8->canonical->convert_blessed->encode($metadata);
	my $ret= "CAS_Dir 04 Unix\n"
		.pack('N', length($meta_json)).$meta_json;

	# Now, build a nice compact string for each entry.
	for my $e (sort {$a->name cmp $b->name} @entries) {
		my $code= $_TypeToCode{$e->type}
			or croak "Unknown directory entry type: ".$e->type;

		my $ref= $e->ref;
		defined $ref or $ref= '';
		
		my @meta= map { defined($e->$_)? $e->$_ : '' } @fieldOrder[3..$#fieldOrder];
		$meta[1]= '' if $meta[1] eq $def_uid;
		$meta[2]= '' if $meta[2] eq $def_gid;
		$meta[3]= '' if $meta[3] eq ($code eq 'd'? $def_dmode : $def_fmode);
		$meta[7]= '' if $meta[7] eq $def_dev;
		my $meta_str= join("\0", @meta);

		my $name= $e->name;
		utf8::encode($name) if utf8::is_utf8($name);
		utf8::encode($ref)  if utf8::is_utf8($ref);
		croak "Name too long: '$name'" if length($name) > 255;
		croak "Value too long: '$ref'" if length($ref) > 255;
		croak "Metadata too long" if length($meta_str) > 255;
		$ret .= pack('CCCA', length($name), length($ref), length($meta_str), $code).$name."\0".$ref."\0".$meta_str."\0";
	}
	croak "Accidental unicode concatenation"
		if utf8::is_utf8($ret);
	$ret;
}

sub decode {
	my ($class, $params)= @_;
	my $handle= $params->{handle};
	if (!$handle) {
		if (defined $params->{bytes}) {
			open($handle, '<', \$params->{bytes})
				or croak "can't open handle to scalar";
		}
		else {
			$handle= $params->{file}->open;
		}
	}

	my $header_len= $class->_calc_header_length($params->{format});
	seek($handle, $header_len, 0) or croak "seek: $!";

	my (@entries, $buf, $pos);

	# first, pull out the metadata, which includes the UID map, the GID map, and the default attributes.
	$class->_readall($handle, $buf, 4);
	my ($dirmeta_len)= unpack('N', $buf);
	$class->_readall($handle, my $json, $dirmeta_len);
	my $enc= JSON->new()->utf8->canonical->convert_blessed;
	DataStore::CAS::FS::NonUnicode->add_json_filter($enc);
	my $meta= $enc->decode($json);

	# Quick sanity checks
	exists $meta->{_}{def}{uid}
		and ref $meta->{_}{umap}
		and ref $meta->{_}{gmap}
		or croak "Incorrect directory metadata";

	while (!eof $handle) {
		$class->_readall($handle, $buf, 4);
		my ($name_len, $ref_len, $meta_len, $code)= unpack('CCCA', $buf);
		$class->_readall($handle, $buf, $name_len+$ref_len+$meta_len+3);
		my @fields= ( map { length($_)? $_ : undef }
			$meta->{_},
			$code,
			substr($buf, 0, $name_len),
			substr($buf, $name_len+1, $ref_len),
			split("\0", substr($buf, $name_len+$ref_len+2, $meta_len), -1),
		);
		push @entries, bless(\@fields, __PACKAGE__.'::Entry');
	}
	close $handle;
	return DataStore::CAS::FS::Dir->new(
		file => $params->{file},
		format => $params->{format},
		entries => \@entries,
		metadata => $meta
	);
}

package DataStore::CAS::FS::DirCodec::Unix::Entry;
use strict;
use warnings;
use parent 'DataStore::CAS::FS::DirEnt';

sub _dirmeta        { $_[0][0] }
sub type            { $_CodeToType{$_[0][1]} }
sub name            { $_[0][2] }
sub ref             { $_[0][3] }
sub size            { $_[0][4] }
sub unix_uid        { length($_[0][5])? $_[0][5] : $_[0][0]{def}{uid} }
sub unix_gid        { length($_[0][6])? $_[0][6] : $_[0][0]{def}{gid} }
sub unix_mode       { length($_[0][7])? $_[0][7] : $_[0][0]{def}{($_[0][1] eq 'd'? 'dmode':'fmode')} }
sub unix_atime      { $_[0][8] }
sub unix_mtime      { $_[0][9] }
sub unix_ctime      { $_[0][10] }
sub unix_dev        { length($_[0][11])? $_[0][11] : $_[0][0]{def}{dev} }
sub unix_inode      { $_[0][12] }
sub unix_nlink      { $_[0][13] }
sub unix_blocksize  { $_[0][14] }
sub unix_blockcount { $_[0][15] }

*modify_ts = *unix_mtime;
sub unix_user       { $_[0][0]{umap}{ $_[0]->unix_uid } }
sub unix_group      { $_[0][0]{gmap}{ $_[0]->unix_gid } }

sub as_hash {
	my $self= shift;
	return { map { $_ => $self->$_() } qw: type name ref size unix_uid
		unix_gid unix_mode unix_atime unix_mtime unix_ctime unix_dev
		unix_inode unix_nlink unix_blocksize unix_blockcount : };
}

1;