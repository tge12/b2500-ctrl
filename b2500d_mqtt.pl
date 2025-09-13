#!/usr/bin/perl

use strict;
use warnings;
use Net::MQTT::Simple;
use Fcntl ':flock';
use HTTP::Tiny;

# exit(0);

my $debug = 1;
my $timeout = 10;

my $mac = '************';  # TODO
my $mqttServer = 'localhost';
my $mqttUser = '********'; # TODO
my $mqttPw = '*******'; # TODO
my $mqttTopicSub = "hame_energy/HMJ-2/device/${mac}/ctrl";
my $mqttTopicPub = "hame_energy/HMJ-2/App/${mac}/ctrl";
my $mqttCmd = 'cd=01';

my $vzUuidWR = '************************************'; # TODO
my $vzBaseUrl = "http://*************/data/${vzUuidWR}.json?operation=add"; # TODO

# w1/2 - pv in power W
# pe - batt %
# kn - batt Wh
# g1/2 - output power W
my @fieldsWanted = ( 'w1', 'w2', 'pe', 'kn', 'g1', 'g2' );
my $testData = "p1=0,p2=0,w1=0,w2=0,pe=33,vv=110,sv=9,cs=0,cd=0,am=0,o1=0,o2=0,do=85,lv=0,cj=1,kn=739,g1=0,g2=0,b1=0,b2=0,md=0,d1=0,e1=20:30,f1=23:59,h1=80,d2=0,e2=0:0,f2=8:0,h2=80,d3=0,e3=8:0,f3=20:0,h3=80,sg=0,sp=80,st=0,tl=27,th=27,tc=0,tf=0,fc=202310231502,id=5,a0=33,a1=0,a2=0,l0=0,l1=0,c0=3,c1=4,bc=307,bs=489,pt=726,it=797,m0=0,m1=0,m2=0,m3=0,d4=0,e4=0:0,f4=23:59,h4=80,d5=0,e5=0:0,f5=23:59,h5=80,lmo=1943,lmi=1407,lmf=1,uv=107,sm=0,bn=0,ct_t=4,tc_dis=0";

my $logFile = "/var/log/b2500d-meter.log";
open( my $logFh, ">>", $logFile ) || die "Can't open $logFile $!";
flock($logFh, LOCK_EX|LOCK_NB) or exit(0);

$logFh->autoflush(1);

print $logFh localtime . " Connecting MQTT on $mqttServer ...\n" if $debug;
my $mqtt = Net::MQTT::Simple->new($mqttServer);

sub printValues($$)
{
  my ($topic, $message) = @_;
  return if ! $message;
  print $logFh localtime . " Received data: $message\n" if $debug;

  my $pvProd = 0;

  my @values = split(/,/, $message);
  for my $kv (@values)
  {
    my ($k, $v) = split(/=/, $kv);
    if(grep(/$k/, @fieldsWanted))
    {
      print("$k : $v\n");
      if($k eq "g1" || $k eq "g2")
      {
        $pvProd += $v;
      }
    }
  }

  $mqtt->disconnect();

  # If the Battery does not yield output power, the inverter does not return any
  # power values at all - then the VZ graph has no data.
  # So, generate 0 samples here
  print $logFh localtime . " PV Production: $pvProd.\n" if $debug;
  if($pvProd == 0)
  {
    my $now = (time() * 1000);
    my $vz = HTTP::Tiny->new();
    my $vzUrl = "${vzBaseUrl}&ts=${now}&value=0";
    print $logFh localtime . " Posting to VZ: $vzUrl\n" if $debug;
    my $vzResp = $vz->get($vzUrl);
    print $logFh localtime . " VZ response: $vzResp->{status} $vzResp->{reason}\n" if $debug;
  }

  print $logFh localtime . " Exit.\n" if $debug;
  exit(0);
}

# printValues("test", $data);

$ENV{MQTT_SIMPLE_ALLOW_INSECURE_LOGIN} = "true";

print $logFh localtime . " Logging on as $mqttUser ...\n" if $debug;
my $rc = $mqtt->login($mqttUser, $mqttPw);
if($rc ne $mqttUser)
{
  print STDERR "Failed to login as $mqttUser: $rc\n";
  exit(1);
}
print $logFh localtime . " Subscribing for $mqttTopicSub ...\n" if $debug;
$mqtt->subscribe($mqttTopicSub => \&printValues);
print $logFh localtime . " Sending command $mqttCmd to $mqttTopicPub ...\n" if $debug;
$mqtt->publish($mqttTopicPub => $mqttCmd);
print $logFh localtime . " Waiting for data ...\n" if $debug;

my $now = time();
while((time() - $now) < $timeout)
{
  $mqtt->tick(10);
  sleep 1;
}
print $logFh localtime . " Exit.\n" if $debug;
$mqtt->disconnect();
exit(0);

