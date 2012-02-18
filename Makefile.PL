use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'File::CAS',
    AUTHOR              => q{Michael Conrad <mike@nrdvana.net>},
    VERSION_FROM        => 'lib/File/CAS.pm',
    ABSTRACT_FROM       => 'lib/File/CAS.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'File-CAS-*' },
);
