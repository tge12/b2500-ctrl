#!/usr/bin/perl
# --------------------------------------------------------------
# Control one (!) Marstek B2500D storage device by:
# - requesting the actual total consumption/production from a aggregated VZ channel
#   (see https://volkszaehler.org), alternatively use whatever other data source
# - subscribe at a local MQTT for the b2500d metrics arriving
#   (triggered by vzlogger requesting the data anyway). Alternatively query here.
# - calculating from energy demand the required storage output
# - send MQTT command to set that storage output or turn if off/on if limits exceeeded,
#   using one time-based config 00:00-23:59 and continuously modifying this
# This requires a hame-relay running (see https://github.com/tomquist/hame-relay ideally in mode 1,
# mode 2 should also work, but is more unstable because of cloud dependency)
# --------------------------------------------------------------

use strict;
use warnings;
use Net::MQTT::Simple;
use Fcntl ':flock';
use HTTP::Tiny;
use JSON::PP;

my $debug = 1;
my $interval = 10;

# --------------------------------------------------------------
# Feed-in limit - if lower and cannot reduce pOut further, turn off pOut.
# Use case is during night:
#  - Feed-in -25 (pOut == 80W and fridge is on)
#  - Feed-in -40 (pOut == 80W and fridge is off) -> turn off pOut
# --------------------------------------------------------------

my $turnOffLimit = -35;

#  Same for turning on:
my $turnOnLimit = 35;

# Avoid reconfiguring for small changes, delta must be at least 10W:
my $hysteresis = 10;

# --------------------------------------------------------------
# Marstek WR allow also min of 40W (even in reality it is a Hoymiles HM600, for which we cannot go lower than 80W)
# --------------------------------------------------------------

my $pMax = 600;
my $pMin = 40;
# my $pMin = 80;

my $mac = '************'; # TODO

my $mqttServer = 'localhost';
my $mqttUser = '********'; # TODO
my $mqttPw = '***********'; # TODO
my $mqttTopicSub = "hame_energy/HMJ-2/device/${mac}/ctrl";
my $mqttTopicPub = "hame_energy/HMJ-2/App/${mac}/ctrl";
my $mqttCmd = 'cd=01';
my $mqttCmdOff = "cd=20,md=0,a1=0,b1=0:00,e1=23:59,v1=80,a2=0,b2=00:00,e2=23:59,v2=80,a3=0,b3=00:00,e3=23:59,v3=80";
my $mqttCmdOn = "cd=20,md=0,a1=1,b1=0:00,e1=23:59,v1=${pMin},a2=0,b2=00:00,e2=23:59,v2=80,a3=0,b3=00:00,e3=23:59,v3=80";

# --------------------------------------------------------------
# These are the interesting MQTT status response fields:
# w1/2 - pv in power W
# pe - batt %
# g1/2 - output power W
# cd=1 -> auto,  cd=0 -> manual
# do - discharge limit
# manual t1 off: d1=0,e1=0:0,f1=23:59,h1=80
# manual t1 on: d1=1,e1=0:0,f1=23:59,h1=80
# --------------------------------------------------------------
my @statusFields = ( 'w1', 'w2', 'pe', 'g1', 'g2', 'cd', 'd1', 'h1', 'do' );

# --------------------------------------------------------------
# The VZ server:
# --------------------------------------------------------------

my $vzUuid = '************************************'; # L1+L2 TODO
my $vzBaseUrl = "http://***********/data/${vzUuid}.json?tuples=5"; # TODO
my $vzDeltaT = 1;

# Auto:
# hame_energy/HMJ-2/device/<MAC>/ctrl p1=1,p2=1,w1=30,w2=29,pe=53,vv=110,sv=9,cs=0,cd=1,am=0,o1=1,o2=1,do=85,lv=80,cj=2,kn=1187,g1=40,g2=40,b1=0,b2=0,md=1,d1=0,e1=20:30,f1=23:59,h1=80,d2=0,e2=0:0,f2=8:0,h2=80,d3=0,e3=8:0,f3=20:0,h3=80,sg=1,sp=80,st=-4,tl=30,th=31,tc=0,tf=0,fc=202310231502,id=5,a0=53,a1=0,a2=0,l0=1,l1=0,c0=3,c1=4,bc=919,bs=460,pt=1653,it=1023,m0=-5,m1=0,m2=0,m3=80,d4=0,e4=0:0,f4=23:59,h4=80,d5=0,e5=0:0,f5=23:59,h5=80,lmo=1957,lmi=1418,lmf=0,uv=107,sm=0,bn=0,ct_t=4,tc_dis=0
# Manuell alles aus:
# hame_energy/HMJ-2/device/<MAC>/ctrl p1=1,p2=1,w1=26,w2=27,pe=53,vv=110,sv=9,cs=0,cd=0,am=0,o1=0,o2=0,do=85,lv=0,cj=2,kn=1187,g1=0,g2=0,b1=0,b2=0,md=0,d1=0,e1=20:30,f1=23:59,h1=80,d2=0,e2=0:0,f2=8:0,h2=80,d3=0,e3=8:0,f3=20:0,h3=80,sg=0,sp=80,st=0,tl=30,th=31,tc=0,tf=0,fc=202310231502,id=5,a0=53,a1=0,a2=0,l0=2,l1=0,c0=3,c1=4,bc=919,bs=461,pt=1655,it=1025,m0=0,m1=0,m2=0,m3=0,d4=0,e4=0:0,f4=23:59,h4=80,d5=0,e5=0:0,f5=23:59,h5=80,lmo=1958,lmi=1418,lmf=0,uv=107,sm=0,bn=0,ct_t=4,tc_dis=0
# Manuell, erstes Interval auf 80
# hame_energy/HMJ-2/device/<MAC>/ctrl p1=1,p2=1,w1=27,w2=24,pe=53,vv=110,sv=9,cs=0,cd=0,am=0,o1=1,o2=1,do=85,lv=80,cj=2,kn=1187,g1=40,g2=40,b1=0,b2=0,md=0,d1=1,e1=0:0,f1=23:59,h1=80,d2=0,e2=0:0,f2=8:0,h2=80,d3=0,e3=8:0,f3=20:0,h3=80,sg=0,sp=80,st=0,tl=30,th=31,tc=0,tf=0,fc=202310231502,id=5,a0=53,a1=0,a2=0,l0=1,l1=0,c0=3,c1=4,bc=919,bs=461,pt=1656,it=1025,m0=0,m1=0,m2=0,m3=80,d4=0,e4=0:0,f4=23:59,h4=80,d5=0,e5=0:0,f5=23:59,h5=80,lmo=1958,lmi=1418,lmf=0,uv=107,sm=0,bn=0,ct_t=4,tc_dis=0

# -------------------------------
# Set up logging
# -------------------------------

my $logFile = "/var/log/b2500d-ctrl.log";
open( my $logFh, ">>", $logFile ) || die "Can't open $logFile $!";
flock($logFh, LOCK_EX|LOCK_NB) or exit(0);

$logFh->autoflush(1);

# -------------------------------
# Connect MQTT
# -------------------------------

print $logFh localtime . " Connecting MQTT on $mqttServer ...\n" if $debug;
my $mqtt = Net::MQTT::Simple->new($mqttServer);

# -------------------------------
# Create HTTP Client
# -------------------------------

print $logFh localtime . " Creating HTTP client $mqttServer ...\n" if $debug;
my $vz = HTTP::Tiny->new();
my $json = JSON::PP->new->ascii->pretty->allow_nonref;

# -------------------------------
# Function getting consumption from VZ
# -------------------------------

sub getConsumption()
{
  # curl 'http://<VZ Server>/data/<VZ channel UUID>.json?unique=1754387122953&from=1754386935678&to=1754387120007&tuples=5' 
  #   {"version":"0.3","data":{"tuples":[[1754387030029,87.49,1],[1754387034300,87.764,1],[1754387091496,87.43,1],[1754387096807,87.485,1],[1754387122234,88.586,1],
  #     [1754387126524,88.586,1],[1754387625387,88.586,1]],"uuid":"<UUID>","from":1754385372665,"to":1754387625387,
  #     "min":[1754387091496,87.43012831077813],"max":[1754387122234,88.5861477837886],"average":87.746,"consumption":54.907,"rows":7}}

  # -----------------------------------------
  # Construct VZ URL and Get it
  # -----------------------------------------

  my $now = (time() * 1000);
  my $tFrom = ($now - ($vzDeltaT * 60000));
  my $vzUrl = "${vzBaseUrl}&unique=${now}&from=${tFrom}&to=${now}";
  print $logFh localtime . " Sending GET to $vzUrl ...\n" if $debug;
  my $vzResp = $vz->get($vzUrl);
  print $logFh localtime . " VZ response: $vzResp->{status} $vzResp->{reason}\n" if $debug;
  if($vzResp->{status} == 200)
  {
    # print $logFh localtime . " VZ response content: $vzResp->{content}\n" if $debug;
    my $vzCont = $json->decode($vzResp->{content});
    # print $logFh localtime . " VZ data: $vzCont\n" if $debug;

    my $idx = scalar(@{$vzCont->{data}->{tuples}}) - 1;
    # print $logFh localtime . " VZ data idx: $idx\n" if $debug;
    my $vzVal = $vzCont->{data}->{tuples}[$idx][1];
    print $logFh localtime . " Extracted VZ value: $vzVal\n" if $debug;
    return $vzVal;
  }

  return undef;
}

# -------------------------------
# Function sending MQTT command
# -------------------------------

sub sendCommand($)
{
  my ($msg) = @_;
  print $logFh localtime . " Sending MQTT command: $msg\n" if $debug;
  $mqtt->publish($mqttTopicPub => $msg);
}

# -------------------------------
# Function handling MQTT data
# -------------------------------

sub handleValues($$)
{
  my ($topic, $message) = @_;
  return if ! $message;

  print $logFh localtime . " Received MQTT data: $message\n" if $debug;
  my @values = split(/,/, $message);
  my $valHash = {};
  for my $kv (@values)
  {
    my ($k, $v) = split(/=/, $kv);
    if(grep(/$k/, @statusFields))
    {
      $valHash->{$k} = $v;
    }
  }

  # cd=1 -> auto,  cd=0 -> manual
  # manual t1 off: d1=0,e1=0:0,f1=23:59,h1=80
  # manual t1 on:  d1=1,e1=0:0,f1=23:59,h1=80
  my $dischargeIsAuto = ($valHash->{cd} == 1);
  my $time1Enabled    = ($valHash->{d1} == 1);
  my $time1Power      = $valHash->{h1};
  my $pctFull         = $valHash->{pe};
  my $dischargeLimit  = $valHash->{do};

  print $logFh localtime . " Discharge mode: " . ($dischargeIsAuto ? "auto" : "time") .
        " t1Enabled: " . ($time1Enabled ? "true" : "false") . " t1Power: " . $time1Power . "\n" if $debug;
  my $consumption = getConsumption();
  if(! $consumption || $consumption == 0)
  {
    # Happens sometimes - ignore this
    print $logFh localtime . " Consumption unknown: 0. No change.\n" if $debug;
    return;
  }

  if($dischargeIsAuto)
  {
    # Enforce manual mode
    print $logFh localtime . " Auto-discharge mode - turning on minimum time-based: ${pMin}.\n" if $debug;
    sendCommand($mqttCmdOn);
    return;
  }

  if($time1Enabled && ($consumption < $turnOffLimit) && ($time1Power == $pMin))
  {
    # Time-based output is ON
    # && we are feeding in more than the turn-off-limit (maybe -40)
    # && we cannot reduce pOut further
    print $logFh localtime . " Turning off output (Cons: $consumption, Limit: $turnOffLimit, pOut: $time1Power).\n" if $debug;
    sendCommand($mqttCmdOff);
    return;
  }

  if($pctFull <= (100 - $dischargeLimit))
  {
    if($time1Enabled)
    {
      print $logFh localtime . " Turning off output (pctFull: $pctFull, dischargeLimit: $dischargeLimit).\n" if $debug;
      sendCommand($mqttCmdOff);
    }
    else
    {
      print $logFh localtime . " Output already off (pctFull: $pctFull, dischargeLimit: $dischargeLimit).\n" if $debug;
    }
    return;
  }

  if(! $time1Enabled && ($consumption > $turnOnLimit))
  {
    # Time-based output is OFF
    # && we are consuming more than the turn-on-limit (maybe 35)
    print $logFh localtime . " Turning on output (Cons: $consumption, Limit: $turnOnLimit, pOut: $pMin).\n" if $debug;
    sendCommand($mqttCmdOn);
    return;
  }
  
  if($time1Enabled && ($consumption < ($hysteresis * -1)) && ($time1Power > $pMin))
  {
    # Feeding in and we can still reduce pOut
    my $pOut = int($time1Power + $consumption);
    if($pOut < $pMin) { $pOut = $pMin; }

    print $logFh localtime . " Reducing output ${time1Power} -> ${pOut}.\n" if $debug;
    my $mqttCmd = "cd=20,md=0,a1=1,b1=0:00,e1=23:59,v1=${pOut},a2=0,b2=00:00,e2=23:59,v2=80,a3=0,b3=00:00,e3=23:59,v3=80";
    sendCommand($mqttCmd);
    return;
  }

  if($time1Enabled && ($consumption > $hysteresis) && ($time1Power < $pMax))
  {
    # Feeding in and we have to increase pOut
    my $pOut = int($time1Power + $consumption);
    if($pOut > $pMax) { $pOut = $pMax; }

    print $logFh localtime . " Increasing output ${time1Power} -> ${pOut}.\n" if $debug;
    my $mqttCmd = "cd=20,md=0,a1=1,b1=0:00,e1=23:59,v1=${pOut},a2=0,b2=00:00,e2=23:59,v2=80,a3=0,b3=00:00,e3=23:59,v3=80";
    sendCommand($mqttCmd);
    return;
  }

  print $logFh localtime . " Not changing anything.\n" if $debug;
}

# -------------------------------
# Connect MQTT and subscribe
# -------------------------------

$ENV{MQTT_SIMPLE_ALLOW_INSECURE_LOGIN} = "true";

print $logFh localtime . " Logging on as $mqttUser ...\n" if $debug;
my $rc = $mqtt->login($mqttUser, $mqttPw);
if($rc ne $mqttUser)
{
  print STDERR "Failed to login as $mqttUser: $rc\n";
  exit(1);
}
print $logFh localtime . " Subscribing for $mqttTopicSub ...\n" if $debug;
$mqtt->subscribe($mqttTopicSub => \&handleValues);

# -------------------------------
# Main loop - do nothing. The subscription callback gets called when MQTT data arrives
# -------------------------------

while(1)
{
  # Not needed - done vzlogger anyway ...
  # print $logFh localtime . " Sending command $mqttCmd to $mqttTopicPub ...\n" if $debug;
  # $mqtt->publish($mqttTopicPub => $mqttCmd);

  $mqtt->tick(10);
  sleep($interval);
}
print $logFh localtime . " Exit.\n" if $debug;
$mqtt->disconnect();
exit(0);

