package File::CAS::Dir::Unix;

use 5.006;
use strict;
use warnings;

use Carp;

our $VERSION= 1.0000;
use parent 'File::CAS::Dir';

__PACKAGE__->RegisterFormat(__PACKAGE__, __PACKAGE__);

=head1 FACTORY FUNCTIONS

=head1 METHODS

=head2 $class->SerializeEntries( \@entries, \%metadata )


=cut

our %_TypeToCode= ( file => 'f', dir => 'd', symlink => 'l', chardev => 'c', blockdev => 'b', pipe => 'p', socket => 's' );
our %_CodeToType= map { $_TypeToCode{$_} => $_ } keys %_TypeToCode;
our %_ValFieldForType= ( f => 'hash', d => 'hash', l => 'linkTarget', c => 'device', b => 'device', p => '', s => '' );
our @fieldOrder= qw( code name value size unix_uid unix_gid unix_mode unix_atime unix_mtime unix_ctime unix_dev unix_inode unix_nlink unix_blocksize unix_blockcount );
sub SerializeEntries {
	my ($class, $entryList, $metadata)= @_;
	
	# Often, an entire directory will have the same permissions for all entries
	#  or vary only by file/directory type.
	# First we find the default value by whichever appears the most often.
	
	my %occur;
	defined($_->{unix_uid}) and $occur{$_->{unix_uid}}++ for @$entryList;
	my ($def_uid)= sort { $occur{$a} > $occur{$b} } keys %occur;
	$def_uid= '' unless defined $def_uid;
	
	%occur= ();
	defined($_->{unix_gid}) and $occur{$_->{unix_gid}}++ for @$entryList;
	my ($def_gid)= sort { $occur{$a} > $occur{$b} } keys %occur;
	$def_gid= '' unless defined $def_gid;
	
	%occur= ();
	defined($_->{unix_mode}) and $_->{type} ne 'dir' and $occur{$_->{unix_mode}}++ for @$entryList;
	my ($def_fmode)= sort { $occur{$a} > $occur{$b} } keys %occur;
	$def_fmode= '' unless defined $def_fmode;
	
	%occur= ();
	defined($_->{unix_mode}) and $_->{type} eq 'dir' and $occur{$_->{unix_mode}}++ for @$entryList;
	my ($def_dmode)= sort { $occur{$a} > $occur{$b} } keys %occur;
	$def_dmode= '' unless defined $def_dmode;
	
	%occur= ();
	defined($_->{unix_dev}) and $occur{$_->{unix_dev}}++ for @$entryList;
	my ($def_dev)= sort { $occur{$a} > $occur{$b} } keys %occur;
	$def_dev= '' unless defined $def_dev;
	
	my $defaultMeta= join("\0", $def_uid, $def_gid, $def_fmode, $def_dmode, $def_dev)."\0";
	croak "Metadata too long"
		if length($defaultMeta) > 255; # I don't think this can possibly happen.
	
	my $umap= join("", map { (defined $_->{unix_uid} && defined $_->{unix_user})? ($_->{unix_uid}."\0" => $_->{unix_user}."\0") : () } @$entryList);
	my $gmap= join("", map { (defined $_->{unix_gid} && defined $_->{unix_group})? ($_->{unix_gid}."\0" => $_->{unix_group}."\0") : () } @$entryList);
	
	my $ret= "CAS_Dir 14 File::CAS::Dir::Unix\n"
		.pack('NNC', length($umap), length($gmap), length($defaultMeta))
		.$umap.$gmap.$defaultMeta;
	
	for my $e (sort {$a->{name} cmp $b->{name}} @$entryList) {
		my $code= $_TypeToCode{$e->{type}}
			or croak "Unknown directory entry type: $e->{type}";
		
		my $val= $e->{$_ValFieldForType{$code}};
		defined $val or $val= '';
		
		my @meta= map { defined($e->{$_})? $e->{$_} : '' } @fieldOrder[3..$#fieldOrder];
		$meta[3]= '' if $meta[3] eq $def_uid;
		$meta[4]= '' if $meta[4] eq $def_gid;
		$meta[5]= '' if $meta[5] eq ($code eq 'd'? $def_dmode : $def_fmode);
		$meta[9]= '' if $meta[9] eq $def_dev;
		my $metaStr= join("\0", @meta)."\0";
		
		croak "Name too long: '$e->{name}'" if length($e->{name}) > 255;
		croak "Value too long: '$val'" if length($val) > 255;
		croak "Metadata too long: '$metaStr'" if length($metaStr) > 255;
		$ret .= pack('CCCA', length($e->{name}), length($val), length($metaStr), $code).$e->{name}."\0".$val."\0".$metaStr;
	}
	
	$ret;
}

=head2 $class->_ctor( \%params )

Private-ish constructor.  Like "new" with no error checking, and requires a blessable hashref.

Defined parameters are "file" and "format".

=cut
sub _ctor {
	my ($class, $params)= @_;
	bless $params, $class;
}

sub _build__entries {
	my $self= shift;
	
	$self->file->seek($self->_headerLenForFormat($self->{format}))
		or croak "seek: $!";
	
	my (@entries, $buf, $pos);
	
	# first, pull out the metadata, which includes the UID map, the GID map, and the default attributes.
	$self->file->readall($buf, 9);
	my ($umapLen, $gmapLen, $defaultsLen)= unpack('NNC', $buf);
	my $meta= $self->{_meta}= {};
	
	$self->file->readall($buf, $umapLen);
	$meta->{umap}= length($buf)? { split("\0", substr($buf, 0, -1), -1) } : {};
	
	$self->file->readall($buf, $gmapLen);
	$meta->{gmap}= length($buf)? { split("\0", substr($buf, 0, -1), -1) } : {};
	
	$self->file->readall($buf, $defaultsLen);
	@{$meta}{qw:def_uid def_gid def_fmode def_dmode def_dev:}= split("\0", substr($buf, 0, -1), -1);
	
	# make sure we got values for all of the defaults
	defined $meta->{def_dev}
		or croak "Error in encoded directory metadata: '".$self->file->hash."'";
	
	while (!$self->file->eof) {
		$self->file->readall($buf, 4);
		my ($nameLen, $valLen, $metaLen, $code)= unpack('CCCA', $buf);
		$self->file->readall($buf, $nameLen+$valLen+$metaLen+2);
		push @entries, bless [
			$meta,
			$code,
			substr($buf, 0, $nameLen),
			substr($buf, $nameLen+1, $valLen),
			split("\0", substr($buf, $nameLen+$valLen+2, -1), -1),
			], __PACKAGE__.'::Entry';
	}
	$self->file->close; # we're most likely done with it
	\@entries;
}

sub _entries {
	$_[0]{_entries} ||= $_[0]->_build__entries();
}

sub _entryHash {
	$_[0]{_entryHash} ||= { map { $_->name => $_ } @{$_[0]->_entries} };
}

=head2 $ent= $dir->getEntry($name)

Get a directory entry by name.

=cut
sub getEntry {
	$_[0]->_entryHash->{$_[1]};
}


package File::CAS::Dir::Unix::Entry;
use strict;
use warnings;

use File::CAS::Dir;
our @ISA=( 'File::CAS::Dir::Entry' );

sub _dirmeta        { $_[0][0] }
sub type            { $_CodeToType{$_[0][1]} }
sub name            { $_[0][2] }
sub _value          { $_[0][3] }
sub size            { $_[0][4] }
sub unix_uid        { length($_[0][5])? $_[0][5] : $_[0][0]{def_uid} }
sub unix_gid        { length($_[0][6])? $_[0][6] : $_[0][0]{def_gid} }
sub unix_mode       { length($_[0][7])? $_[0][7] : $_[0][0]{($_[0][1] eq 'd'? 'def_dmode':'def_fmode')} }
sub unix_atime      { $_[0][8] }
sub unix_mtime      { $_[0][9] }
sub unix_ctime      { $_[0][10] }
sub unix_dev        { length($_[0][11])? $_[0][11] : $_[0][0]{def_dev} }
sub unix_inode      { $_[0][12] }
sub unix_nlink      { $_[0][13] }
sub unix_blocksize  { $_[0][14] }
sub unix_blockcount { $_[0][15] }

*modify_ts = *unix_mtime;
sub hash            { ($_[0][1] eq 'f' || $_[0][1] eq 'd')? $_[0][3] : undef }
sub linkTarget      { $_[0][1] eq 'l'? $_[0][3] : undef }
sub device          { ($_[0][1] eq 'b' || $_[0][1] eq 'c')? $_[0][3] : undef }
sub unix_user       { $_[0][0]{umap}{ $_[0]->unix_uid } }
sub unix_group      { $_[0][0]{gmap}{ $_[0]->unix_gid } }

1;