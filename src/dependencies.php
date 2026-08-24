<?php
// DIC configuration

$container = $app->getContainer();

// view renderer
$container['renderer'] = function ($c) {
    $settings = $c->get('settings')['renderer'];
    return new Slim\Views\Twig($settings['template_path']);
};

$container['cache'] = function () {
        return new \Slim\HttpCache\CacheProvider();
};

// market database
$container['db'] = function ($c) {
    $settings = $c->get('settings')['db'];
    $db = new PDO($settings['dsn'], $settings['user'], $settings['password']);
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
    return $db;
};


// monolog
$container['logger'] = function ($c) {
    $settings = $c->get('settings')['logger'];
    $logger = new Monolog\Logger($settings['name']);
    $logger->pushProcessor(new Monolog\Processor\UidProcessor());
    $logger->pushHandler(new Monolog\Handler\StreamHandler($settings['path'], Monolog\Logger::DEBUG));
    return $logger;
};
