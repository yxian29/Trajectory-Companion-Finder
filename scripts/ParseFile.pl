#!/usr/bin/perl
use strict;
use warnings;
use Time::HiRes qw(time);

if(@ARGV < 2) {
	die "usage ParseFile [input] [output]";
}
# example: parser Trajectories.txt Trajectories_edit.csv

my $infile = $ARGV[0];
my $outfile = $ARGV[1];

my $objectId = 0;
my $start = time();
open(my $data, '<', $infile) or die "Could not open '$infile' $!\n";
open(my $fh, '>', $outfile) or die "Could not create '$outfile' $!\n";

while(my $line = <$data>) {
	chomp $line;

	if($line =~ /:/) {
		$line =~ tr/0-9//cd;
		$objectId = $line;
		next;
	}

  $line = "$objectId,$line";
  print $fh "$line\n";
}

my $elapsed = time() - $start;
printf("%.2f s\n", $elapsed);

close $fh;
