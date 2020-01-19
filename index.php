<?php


require('Db.php');
require('downloader.php');
require('csvreader.php');


define('DB_NAME', 'FOO_BAR');
define('DB_USER', 'root');
define('DB_PASSWORD', '1234');
define('DB_HOST', 'localhost');


try {

    //$csv = new CSVReader(__DIR__ . "\\46b957db-f8dd-46f8-91af-b637203254b1.csv");
    

    $dowloader = new FileDownloader();
    $newfile = $dowloader->download("https://foobar.csv","temp.csv");
    $config = new DBConfig(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST);
    $DB = new DBContext($config);
    $csv = new CSVReader($newfile,CSVReader::DELIMITER_TAB);
    $DB->InsertCsv($csv);

    echo "done" . PHP_EOL;
} catch (Exception $e) {

    echo $e->getMessage();
}
