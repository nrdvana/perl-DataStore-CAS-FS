package DataStore::CAS::FS::NonUnicode;
use strict;
use warnings;
use Carp;
use overload '""' => \&to_string, 'cmp' => \&str_compare, '.' => \&str_concat;
use Scalar::Util 'refaddr', 'reftype', 'blessed';

=head1 NAME

DataStore::CAS::FS::NonUnicodeOctets - class to wrap non-unicode data to
prevent it from getting mis-coded during serialization.

=head1 SYNOPSIS

  my $j= JSON->new()->convert_blessed
             ->filter_json_single_key_object(
                '*NonUnicode*' => \&DataStore::CAS::FS::NonUnicode::FROM_JSON
               );
  
  my $x= DataStore::CAS::FS::NonUnicode->new("\x{FF}");
  my $json= $j->encode($x);
  my $x2= "".$j->decode($json);
  is( $x, $x2 );
  ok( !utf8::is_utf8($x2) );

=head1 DESCRIPTION

This utility class wraps a string such that it won't accidentally be converted
to unicode.  When encoding Perl strings as JSON, the unicode flag gets lost,
and all strings are interpreted as unicode when deserialized.

This class has a to_json() method that writes
C<{ '*NonUnicode*' => $bytes_as_codepoints }>.  You can check for that when
reading JSON using JSON's C<filter_json_single_key_object> with the FROM_JSON
method.

=head1 METHODS

=head2 new( $byte_str )

Wraps the given scalar with an instance of this class.

Dies if the scalar is actually unicode.

=head2 to_string  #stringify operator

Returns the original string.

=head2 str_compare  #cmp operator

Runs a regular 'cmp'.  Warns if comparing the string to a unicode string.

=head2 str_concat  #'.' operator

Warns if concatenating to a unicode string.  Converts the unicode string to
UTF-8 octets before concatenating, and returns a NonUnicodeOctets object.

=head2 TO_JSON

Called by the JSON module when convert_blessed is enabled.

=head2 FROM_JSON

Pass this function to JSON's C<filter_json_single_key_object> with a key of
'*NonUnicode*' to restore the objects that were serialized.

=cut

sub new {
	my ($class, $str)= (@_ == 1)? (__PACKAGE__,$_[0]) : @_;
	croak "Passed string was actually unicode: '$str'"
		if utf8::is_utf8($str);
	bless \$str, $class;
}

sub to_string { ${$_[0]} }

sub str_compare {
	my ($self, $other, $swap)= @_;
	if (utf8::is_utf8($other)) {
		carp "Comparing Unicode to non-utf octet array";
		utf8::encode($other);
	}
	my $ret= $$self cmp $other;
	return $swap? -$ret : $ret;
}

sub str_concat {
	my ($self, $other, $swap)= @_;
	if (utf8::is_utf8($other)) {
		carp "Concatenating Unicode and non-utf octet array";
		utf8::encode($other);
	}
	return bless \($swap? $other.$$self : $$self.$other), ref($self);
}

sub add_json_filter {
	my ($self, $json)= @_;
	$json->filter_json_single_key_object(
		'*NonUnicode*' => \&DataStore::CAS::FS::NonUnicode::FROM_JSON
	);
}

sub TO_JSON {
	my $x= ${$_[0]};
	utf8::upgrade($x);
	return { '*NonUnicode*' => $x };
}

sub FROM_JSON {
	my $x= $_[0];
	utf8::downgrade($x);
	return __PACKAGE__->new($x);
}

1;

__END__

# The inverse operation, which can be applied to a whole tree
#  after deserializing JSON.
my $_seen= ();
sub RestoreObjects {
	return unless defined $_[1] and ref $_[1];
	local %$_seen= ();
	local $_= $_[1];
	&_restore_recursive;
}
sub _restore_recursive {
	return if $_seen->{blessed($_)? refaddr $_ : $_}++;
	my $r= blessed($_)? reftype $_ : ref $_;
	if ($r eq 'HASH') {
		if (defined $_->{'*NonUnicode*'} and (ref $_ eq 'HASH') and (keys %$_ == 1)) {
			# Found a former instance.  Restore it to being a blessed object.
			my $x= $_->{'*NonUnicode*'};
			utf8::downgrade($x);
			$_= __PACKAGE__->new($x);
		}
		else {
			defined $_ and ref $_ and &_restore_recursive
				for values %$_;
		}
	}
	elsif ($r eq 'ARRAY') {
		defined $_ and ref $_ and &_restore_recursive
			for @$_;
	}
	elsif ($r eq 'REF' and defined $$_ and ref $$_) {
		local $_= $$_;
		&_restore_recursive
	}
}

1;