#!/usr/bin/env perl
use strict;
use warnings;
use Path::Tiny;
use Data::Dumper;
#use LWP::Simple;
use LWP::UserAgent;
use HTTP::Cookies::Netscape;

#get xml files of large groups
my @jgi_zomes = qw(
http://genome.jgi.doe.gov/ext-api/downloads/get-directory?organism=Metazome
http://genome.jgi.doe.gov/ext-api/downloads/get-directory?organism=PhytozomeV10
http://genome.jgi-psf.org/ext-api/downloads/get-directory?organism=fungi
http://genome.jgi-psf.org/ext-api/downloads/get-directory?organism=fungi
);

my $OUT = $ARGV[0] or path('.');
my $cookie_path = path($OUT)->parent;
$cookie_path = path($cookie_path, 'cookie_jgi');
my $cookie_jar = HTTP::Cookies::Netscape->new(
   file => "$cookie_path",
);
my $ua = LWP::UserAgent->new;
$ua->cookie_jar( $cookie_jar );
print Dumper($ua);

foreach my $URL (@jgi_zomes) {
	(my $xml_name = $URL) =~ s{\A(?:.+?)organism=(.+)\z}{$1};
	my $xml_path = path($OUT, $xml_name . '.xml')->canonpath;
	#getstore($URL, $xml_path);

	my $req = HTTP::Request->new(
		GET => $URL
    );
	my $res = $ua->request($req, $xml_path);
	if ($res->is_success) {
		print "ok\n";
	}
	else {
		print $res->status_line, "\n";
	}


}

