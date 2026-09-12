--TEST--
A malformed progress packet throws ProtocolException instead of returning an incomplete result
--EXTENSIONS--
clickhouse
pcntl
--FILE--
<?php
use ClickHouse\Driver\{Client, ClientOptions};
use ClickHouse\Driver\Exception\ProtocolException;

$server = stream_socket_server('tcp://127.0.0.1:0');
$address = stream_socket_get_name($server, false);
$port = (int) substr(strrchr($address, ':'), 1);
$pid = pcntl_fork();
if ($pid === -1) {
    throw new RuntimeException('Unable to fork the scripted server');
}
if ($pid === 0) {
    $peer = stream_socket_accept($server, 5);
    stream_set_timeout($peer, 5);
    fread($peer, 8192);
    // ServerHello at revision 50000, followed by a progress packet with an invalid rows varint.
    fwrite($peer, "\x00\x02CH\x01\x01\xd0\x86\x03");
    fread($peer, 8192);
    fwrite($peer, "\x03" . str_repeat("\x80", 10));
    stream_socket_shutdown($peer, STREAM_SHUT_WR);
    stream_get_contents($peer);
    fclose($peer);
    fclose($server);
    exit(0);
}

fclose($server);
try {
    $client = new Client(new ClientOptions('127.0.0.1', $port));
    $client->select('SELECT 1');
    echo "Incomplete result accepted\n";
} catch (ProtocolException $e) {
    echo $e->getMessage() . "\n";
} finally {
    unset($client);
    pcntl_waitpid($pid, $status);
}
?>
--EXPECT--
can't read progress packet from input stream
