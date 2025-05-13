<?php
require __DIR__ . '/../vendor/autoload.php';

use Andydixon\Chronotheus\Proxy;

try {
    $proxy = new Proxy();
    $proxy->handle(
        $_SERVER['REQUEST_URI'],
        $_SERVER,
        file_get_contents('php://input')
    );
} catch (\Throwable $e) {
    error_log(sprintf(
        "[Chronotheus] Uncaught exception: %s in %s on line %d\n%s",
        $e->getMessage(), $e->getFile(), $e->getLine(), $e->getTraceAsString()
    ));
    http_response_code(500);
    header('Content-Type: application/json');
    echo json_encode(['status' => 'error', 'error' => 'Internal server error']);
}