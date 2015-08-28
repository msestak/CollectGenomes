#!/usr/bin/env perl
use strict;
use warnings;
use 5.010;
use Data::Dumper;


	my $FILENAME = q{Acoerulea_195_v1.1.protein.fa.gz};
	my ($first_letter, $rest) = $FILENAME =~ m{\A(.)([^_]+).+\z};
	my $species_pattern = $first_letter . '%' . $rest;
	say "PATTERN:$species_pattern";

	my ($species_from_file) = $FILENAME =~ m{\A([^_]+).+\z};
	say "SPECIES_FROM_NAME:$species_from_file";



	my @species = qw(Arnebia_coerulea Aquilegia_coerulea);
	say "SPECIES:@species";

	my %species_short = map { $_ =~ m{\A(.)(?:[^_]+)_(.+)\z};  $1 . $2 => $_; } @species;
	say Dumper(\%species_short);
	

