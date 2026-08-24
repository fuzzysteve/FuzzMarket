<?php
return [
    'settings' => [
        'displayErrorDetails' => false, // set to false in production

        // Renderer settings
        'renderer' => [
            'template_path' => __DIR__ . '/../templates/',
        ],

        // Monolog settings
        'logger' => [
            'name' => 'slim-app',
            'path' => __DIR__ . '/../logs/app.log',
        ],

        // Market database settings
        'db' => [
            'dsn' => 'pgsql:host=localhost;dbname=marketdata',
            'user' => getenv('MARKETDATA_DB_USER') ?: 'marketdata',
            'password' => getenv('MARKETDATA_DB_PASSWORD') ?: 'marketdatapass',
        ],
    ],
];
