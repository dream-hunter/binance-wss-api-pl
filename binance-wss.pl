#!/usr/bin/env perl

use strict;
use IO::Async::Loop;
use Net::Async::WebSocket::Client;
use IO::Async::Timer::Periodic;
use JSON;
use POSIX;
use Data::Dumper;

my $host = "stream.binance.com";
my $port = "443";
my $heartbeat_interval = 300;
my $heartbeat;
#################################
# Async Web Socket Client handler
#################################
while (1) {
    my $timer  = undef;
    my $client = undef;
    my $loop   = undef;

    $heartbeat = time+$heartbeat_interval;

    $timer = IO::Async::Timer::Periodic->new(
        interval=> 10,
        on_tick => sub {
            if ($heartbeat < time) {
                print strftime("%Y-%m-%d %H:%M:%S ", localtime);
                print "Close connection...\n";
                $client->close_now;
                $timer->stop;
                $loop->loop_stop;
            } else {
                print strftime("%Y-%m-%d %H:%M:%S ", localtime);
                printf ("Heartbeat is fine %s > %s\n", $heartbeat, time) ;
            }
        }
    );

    $client = Net::Async::WebSocket::Client->new(
        on_ping_frame => sub {
            my ( $bytes ) = $_[0];
            printf ("%s %s", strftime("%Y-%m-%d %H:%M:%S ",localtime), "Ping frame received - sending Pong\n");
            $client->send_pong_frame($bytes);
            $heartbeat = time+$heartbeat_interval;
        },
        on_text_frame => sub {
            my $data = dataHandler(@_);
        }
    );

    $loop = IO::Async::Loop->new;

    $timer->start;
    $loop->add( $timer );

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

    sleep 10;
    print strftime("%Y-%m-%d %H:%M:%S ", localtime);
    print "Start again\n";
}
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