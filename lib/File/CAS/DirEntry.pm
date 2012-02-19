package File::CAS::DirEntry;

use 5.006;
use strict;
use warnings;

use DateTime;

sub name { undef; }
{ no strict 'refs';
  *{$_}= *name for qw: type hash size create_ts modify_ts linkTarget
	uid gid mode atime ctime unix_dev unix_inode unix_nlink unix_blocksize unix_blocks :;
}

sub createDate { DateTime->from_epoch( epoch => $_[0][4] ) }
sub modifyDate { DateTime->from_epoch( epoch => $_[0][5] ) }

1;