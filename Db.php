<?php


class DBConfig
{


    private $DB_NAME;
    private $DB_USER;
    private $DB_PASSWORD;
    private $DB_HOST;

    public function __construct($db_name, $db_user, $db_passwd, $db_host)
    {
        $this->DB_NAME = $db_name;
        $this->DB_USER = $db_user;
        $this->DB_PASSWORD = $db_passwd;
        $this->DB_HOST = $db_host;
    }

    private function IsNullOrEmptyString($str)
    {
        return (!isset($str) || trim($str) === "");
    }

    public function GetConnectionStringMysql()
    {
        return "mysql:host=" . $this->DB_HOST . ";";
    }

    public function GetUser()
    {
        return $this->DB_USER;
    }

    public function GetPassword()
    {
        return $this->DB_PASSWORD;
    }
    public function GetDBName()
    {
        return $this->DB_NAME;
    }

    public function IsValid()
    {

        return  !$this->IsNullOrEmptyString($this->DB_NAME)
            && !$this->IsNullOrEmptyString($this->DB_USER)
            && !$this->IsNullOrEmptyString($this->DB_PASSWORD)
            && !$this->IsNullOrEmptyString($this->DB_HOST);
    }
}

class DBContext
{
    private $conn = NULL;
    private $config = NULL;

    private $db_columns = array("product_id", "article", "name", "url", "image", "Price", "Price_old");

    public function __construct($newconfig)
    {
        $this->config = $newconfig;
    }


    private function CreateDB()
    {
        $sql = "CREATE DATABASE IF NOT EXISTS " .  $this->config->GetDBName();
        $this->conn->exec($sql);
        $sql = "use " . $this->config->GetDBName();
        $this->conn->exec($sql);


        $sql = "CREATE TABLE IF NOT EXISTS `" . $this->config->GetDBName() . "`.`articles` (
                `product_id` INT NOT NULL AUTO_INCREMENT,
                `article` INT NOT NULL,
                `name` TINYTEXT NULL,
                `url` TEXT NULL,
                `image` TEXT NULL,
                `Price` FLOAT NULL,
                `Price_old` FLOAT NULL,
                `savings_amount` FLOAT NULL,
                PRIMARY KEY (`product_id`),
                UNIQUE KEY `article_UNIQUE` (`article`)
                );";


        $this->conn->exec($sql);

        //DROP triggers if exists
        $sql = "DROP TRIGGER IF EXISTS " . $this->config->GetDBName() . ".after_articles_insert;
                DROP TRIGGER IF EXISTS " . $this->config->GetDBName() . ".after_articles_update;";
        $this->conn->exec($sql);

        //create triggers for savings amount
        $sql = "
                CREATE TRIGGER after_articles_insert
                BEFORE INSERT
                ON " . $this->config->GetDBName() . ".articles FOR EACH ROW 
                BEGIN
                    SET @diff = NEW.Price - NEW.Price_old; 
                    IF @diff > 0 THEN
                        SET NEW.savings_amount = @diff;
                    ELSE
                        SET NEW.savings_amount = 0;
                    END IF;
                END;";
        $this->conn->exec($sql);

        $sql = "
                CREATE TRIGGER after_articles_update
                BEFORE UPDATE
                ON " . $this->config->GetDBName() . ".articles FOR EACH ROW 
                BEGIN
                    SET @diff = NEW.Price - NEW.Price_old; 
                    IF @diff > 0 THEN
                        SET NEW.savings_amount = @diff;
                    ELSE
                        SET NEW.savings_amount = 0;
                    END IF;
                END;";
        $this->conn->exec($sql);
    }

    private function connect()
    {
        $d = $this->config->GetConnectionStringMysql();
        $this->conn = new PDO($this->config->GetConnectionStringMysql(), $this->config->GetUser(), $this->config->GetPassword());
        $this->conn->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
        $this->conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }

    private function ensureDB()
    {
        if (!$this->config->IsValid()) {
            throw new Exception(
                "Invalid DB Config" . var_dump($this->config)
            );
        }

        $this->connect();
        $this->CreateDB();
    }

    public function InsertCsv($csv)
    {
        $this->ensureDB();

        if (!$csv) {
            throw new Exception(
                "Cannot insert to DB: invalid csv Object"
            );
        }
        if (!$csv instanceof CSVReader) {
            throw new Exception(
                "Cannot insert to DB: expected 'class CSVReader' as csv object"
            );
        }


        $FilteredColNames = $csv->GetFilteredHeader();
        //INSERT INTO table ( col1,col2,col3,col4,col5)
        $ColumnNames = implode(', ', $FilteredColNames);


        if (!$this->AreColumnNamesValid($FilteredColNames)) {
            throw new Exception("Expected Colnames: " . implode(",", $this->db_columns) . " Colnames received: " .  $ColumnNames);
        }

        $OnUpdateSql = $this->CreateUpdateStatement($FilteredColNames);

        $ValuesSql = " ";
        $count = 1;
        $QueueCount = 0;
        foreach ($csv as $row) {
            $FilteredRow =  $csv->FilterRowValues($row);
            $quotedValues = $this->QuoteStringArray($FilteredRow);
            //('a','b','c','d','e','f')
            $ValuesSql .= "(" . implode(",", $quotedValues) . ")";


            $count++;
            $QueueCount++;
            //flush query every 500 entrys
            if ($count % 500 === 0) {

                $this->FlushQuery($ColumnNames, $ValuesSql, $OnUpdateSql);
                //reset Values to whitespace
                $ValuesSql = " ";
                $QueueCount = 0;
            } else {
                //add "," between values (a) , (b) exept last 
                $ValuesSql .= ",";
            }
        }

        if ($QueueCount > 0) {
            //remove trailing ","
            $ValuesSql = substr($ValuesSql, 0, -1);
            $this->FlushQuery($ColumnNames, $ValuesSql, $OnUpdateSql);
        }

        echo "Processed $count lines" . PHP_EOL;
    }


    /**
     * 
     *  @returns "UPDATE col1 = values(col1), col2= values(col2) ....."
     * 
     * */
    private function CreateUpdateStatement($Cols)
    {
        $OnUpdateSql = " ";
        $count = 0;
        $len = count($Cols);
        foreach ($Cols as $key) {

            $OnUpdateSql .=  $key . "=values(" . $key . ")";
            //add "," exept last element
            $count++;
            if ($count !== $len) {
                $OnUpdateSql .= ",";
            }
        }
        return $OnUpdateSql;
    }

    /**
     * @returns Quoted string array
     */
    private function QuoteStringArray($arr)
    {
        $quotedValues = [];
        foreach ($arr as $value) {
            array_push($quotedValues, $this->conn->quote($value));
        }
        return $quotedValues;
    }

    private function AreColumnNamesValid($FilteredColumnNames)
    {
        $valid = true;
        if (count($FilteredColumnNames) < 1) {
            $valid = false;
        }
        foreach ($FilteredColumnNames as $name) {
            if (!in_array($name, $this->db_columns)) {
                $valid = false;
            };
        }

        return $valid;
    }


    private function FlushQuery($col, $val, $Update)
    {

        $sql = "INSERT INTO articles($col)
        VALUES
        $val 
        ON DUPLICATE KEY UPDATE 
        $Update";

        $this->conn->exec($sql);
    }


    /**
     * single row insert
     */

    /*
    private function InsertCsvRow($FilteredRow, $FilteredColNames)
    {

        //INSERT INTO ( col1,col2,col3,col4,col5)
        $ColumnNames = implode(', ', $FilteredColNames);

        // VALUES (:col1,:col2,:col3,:col4)
        $ColumnPreparedValues = ':' . implode(',:', $FilteredColNames);



        // array(col1 => :col1, col2 => :col2)
        $OnUpdate = array_combine($FilteredColNames, explode(",", $ColumnPreparedValues));
        $OnUpdateSql = " ";
        //create array to merge later with distinct names [:col12,:col22,:col32 ...]
        $OnUpdatePrepared = [];
        //ON DUPLCATE KEY UPDATE col1 = :col12, col2 = :col22 
        //make the update values distinct
        foreach ($OnUpdate as $key => $value) {

            $OnUpdateSql .=  $key . "=values(" . $key . ")";
            array_push($OnUpdatePrepared,  $value . "2");

            //add "," exept last element
            end($OnUpdate);
            if ($key !== key($OnUpdate)) {
                $OnUpdateSql .= ",";
            }
        }

        //merge both arrays (col1,col2,col3) with (col12,col22,col32)
        $tempArr = explode(",", $ColumnPreparedValues);
        $mergedkeys = array_merge($tempArr, $OnUpdatePrepared);

        //final query
        /**
         * INSERT INTO table (col1,col2,col3...)
         * VALUES (:col1,:col2,:col3 ...)
         * ON DUPLICATE KEY UPDATE 
         * col1=:col12,col2=col22,col3=col32 ...
         */

    /**
     * example query
     * "INSERT INTO articles(article, name, image, url, Price, Price_old)
     *VALUES (:article,:name,:image,:url,:Price,:Price_old)
     *ON DUPLICATE KEY UPDATE 
     *article=:article2,name=:name2,image=:image2,url=:url2,Price=:Price2,Price_old=:Price_old2"
     */
    /*
        $sql = "INSERT INTO articles($ColumnNames)
              VALUES ($ColumnPreparedValues)
              ON DUPLICATE KEY UPDATE 
              $OnUpdateSql";

        $statement =  $this->conn->prepare($sql);
        $InsertData =  array_combine(explode(",", $ColumnPreparedValues), array_merge($FilteredRow, $FilteredRow)); //merge Values with themselves for update statement
        $statement->execute($InsertData);
    }*/
}
