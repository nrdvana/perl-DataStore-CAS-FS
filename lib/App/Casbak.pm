package App::Casbak;
use strict;
use warnings;
use YAML ();
use File::CAS;
use File::Spec;
use Carp;
use Try::Tiny;

our $VERSION= 0.0100;

sub backupDir { $_[0]{backupDir} }
sub cfgFile   { File::Spec->catfile($_[0]->backupDir, 'casbak.conf.yml') }
sub logFile   { File::Spec->catfile($_[0]->backupDir, 'casbak.log') }
sub rootsFile { File::Spec->catfile($_[0]->backupDir, 'casbak.snapshots') }
sub storeDefaultPath { File::Spec->catdir($_[0]->backupDir, 'store') }

sub snapshots {
	my $self= shift;
	$self->{snapshots} ||= do {
		open my $fh, "<", $self->rootsFile
			or die "Failed to read '".$self->rootsFile."': $!\n";
		my @entries;
		while (<$fh>) {
			my @fields= split /[\t\n]/;
			scalar(@fields) >= 2
				or die "Invalid entry in '".$self->rootsFile."': \"$_\"\n";
			push @entries, \@fields;
		}
		\@entries;
	};
}

sub getSnapshot {
	my ($self, $targetDate)= @_;
	my $array= $self->snapshots;
	
	return undef unless @$array;
	
	# if targetEpoch is undef, return the latest snapshot
	return $array->[-1] unless defined $targetDate;
	
	my ($min, $max, $mid)= (-1, $#$array);
	while ($min < $max) {
		$mid= ($min+$max+1)>>1;
		if ($targetDate ge $array->[$mid][0]) {
			$min= $mid;
		} else {
			$max= $mid-1;
		}
	}
	return $max < 0? undef : $array->[$min];
}

sub getSnapshotOrDie {
	my ($self, $date)= @_;
	$self->getSnapshot($date)
		or die defined $date? "No snapshot existed on date $date\n" : "No snapshots recorded yet\n";
}
	
sub addSnapshot {
	my ($self, $date, $hash)= @_;
	push @{$self->snapshots}, [ $date, $hash ];
	my $tmpFile= $self->rootsFile.'.tmp';
	try {
		open my $fh, ">", $tmpFile
			or die "Cannot write to '$tmpFile': $!\n";
		for my $snap (@{$self->snapshots}) {
			print $fh join("\t", @$snap)."\n";
		}
		close $fh
			or die "Cannot save '$tmpFile': $!\n";
		rename $tmpFile, $self->rootsFile
			or die "Cannot replace '".$self->rootsFile."': $!\n";
	}
	catch {
		die "$_\nThe entry that would have been written is: '$date $hash'\n";
	};
	1;
}

sub config    { $_[0]{config} }

sub casParams { $_[0]->config->{casParams} }

sub cas {
	my $self= shift;
	$self->{cas} ||= File::CAS->new(
			# We don't store the 'path' in the configuration if it was the local 'store' dir.
			defaultPath => $self->storeDefaultPath,
			%{$self->casParams}
		);
}

sub new {
	my $class= shift;
	$class->_ctor({ ref($_[0])? %{$_[0]} : @_ });
}

sub _ctor {
	my ($class, $params)= @_;
	my $self= bless $params, $class;

	$self->{backupDir}= '.' unless defined $self->{backupDir};
	
	# auto-load the configuration unless it was passed as a param
	unless (defined $self->{config}) {
		$self->{config}= YAML::LoadFile($self->cfgFile)
			or die "Failed to load configuration at '".$self->cfgFile."': $!\n";
	}
	
	($self->config->{VERSION}||0) <= $VERSION
		or die "Backup version ".$self->config->{VERSION}." is newer than this module $VERSION !\nAborting\n";
	
	$self;
}

sub dateParser {
	my $self= shift;
	require DateTime;
	require Date::Format::Natural;
	$self->{dateParser} ||= DateTime::Format::Natural->new;
}

sub normalizeDate {
	my ($self, $dateSpec)= @_;
	
	if ($dateSpec =~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}[tT][0-9]{2}:[0-9]{2}:[0-9]{2}Z$/) {
		# no need to load DateTime module, because the date is fine as-is
		return $dateSpec;
	} elsif ($dateSpec =~ /^([0-9]+)([DWMY])$/) {
		require DateTime;
		require DateTime::Duration;
		my %field= ( D => 'days', W => 'weeks', M => 'months', Y => 'years' );
		my $d= DateTime->now->add( DateTime::Duration->new($field{$2} => $1) );
		return $d->ymd().'T'.$d->hms().'Z';
	} else {
		my $dt= $self->dateParser->extract_datetime($dateSpec);
		$self->dateParser->success
			or die "Invalid date: '$dateSpec'\n";
		if ($dt->time_zone eq 'floating') {
			$dt->time_zone('local');
		}
		$dt->time_zone('UTC');
		return $dt->ymd.'T'.$dt->hms.'Z';
	}
}

sub init {
	my ($self, $params)= @_;
	
	my $dir= $self->backupDir;
	my @entries= grep { $_ ne '.' && $_ ne '..' } <$dir/*>;
	scalar(@entries)
		and die "Backups may only be initialized in an empty directory\n";
	
	# for stores which require a filesystem path, we default them to a subdir named 'store'
	$self->{cas}= File::CAS->new(defaultPath => $self->storeDefaultPath, create => 1, %$params);
	$self->config->{casParams}= $params;
	$self->config->{VERSION}= $VERSION;
	
	YAML::DumpFile($self->cfgFile, $self->config)
		or die "Error saving '".$self->cfgFile."': $!\n";
	my $fd;
	open($fd, ">", $self->logFile) && close($fd)
		or die "Failed to write log file: $!\n";
	open($fd, ">", $self->rootsFile) && close($fd)
		or die "Failed to write snapshots file: $!\n";
}

sub ls {
	my ($self, $params)= @_;
	my $date;
	$date= $self->normalizeDate($params->{date}) if ($params->{date});
	my $snap= $self->getSnapshotOrDie($date);
	my $root= $self->cas->getDir($snap->[1])
		or die "Missing root directory '$snap->[1]' !\n";
	for ($root->getEntries) {
		print $_->name."\n";
	}
}

1;