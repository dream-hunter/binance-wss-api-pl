#!/usr/bin/env perl

use strict;
use IO::Async::Loop;
use Net::Async::WebSocket::Client;
use JSON;
use POSIX;
use Data::Dumper;

my $client;
my $host = "stream.binance.com";
my $port = "443";
#################################
# Async Web Socket Client handler
#################################
$client = Net::Async::WebSocket::Client->new(
    on_ping_frame => sub {
        my ( $bytes ) = $_[0];
        printf ("%s %s", strftime("%Y-%m-%d %H:%M:%S ",localtime), "Ping frame received - sending Pong");
        $client->send_pong_frame($bytes);
    },
    on_text_frame => sub {
        my $data = dataHandler(@_);
    }
);

my $loop = IO::Async::Loop->new;

$loop->add( $client );

$client->connect(
   host => $host,
   service => $port,
   url => "wss://$host:$port/stream",
)->get;

print strftime("%Y-%m-%d %H:%M:%S ", localtime);
print "Connected; go ahead...\n";

my $datasend = {"method" => "SUBSCRIBE","params" => ["btcusdt\@aggTrade"],"id" => 1};
$client->send_text_frame( encode_json($datasend) );

$loop->run;
#############
# Subroutines
#############
sub dataHandler {
    my ( $self, $frame ) = @_;
    my $decoded = decode_json($frame);
    if ($decoded->{'data'}->{'e'} eq "aggTrade") {
        printf ("%s %s %s %s %s %s\n",
            strftime("%Y-%m-%d %H:%M:%S", localtime),
            $decoded->{'data'}->{'e'},
            $decoded->{'data'}->{'s'},
            $decoded->{'data'}->{'p'},
            $decoded->{'data'}->{'q'},
            $decoded->{'data'}->{'m'}
        );
    } else {
        print Dumper $decoded;
    };
    return $decoded;
}