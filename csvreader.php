<?php

class CsvToDBModifier
{

    private $HeaderName;

    public $ValidateHandle;
    public $extra;

    public function __construct($headername, $validatefunc, $extra = NULL)
    {
        $this->HeaderName = $headername;
        $this->ValidateHandle =  $validatefunc;
        $this->extra = $extra;
    }

    public function IsHeaderNameValid()
    {
        return !is_null($this->HeaderName);
    }

    public function IsValidateFunctionValid()
    {
        return !is_null($this->ValidateHandle);
    }

    public function Validate($value)
    {
        return call_user_func($this->ValidateHandle, $value);
    }

    public function GetHeaderName()
    {
        return $this->HeaderName;
    }

    public static function IS_INT_ELSE_THROW_ECXEPTION($value)
    {
        if (is_numeric($value)) {
            $i = intval($value);
            if ($i >= 0) {
                return $i;
            }
        }
        $error = "";
        if ($this->extra instanceof CSVReader){
           $error = "LineNumber" . $this->extra->key();
        }
        throw new Exception("exptected integer on: $this->HeaderName | $error");
    }

    public static function IS_INT_GREATER_EQUAL_ZERO($value)
    {
        if (is_numeric($value)) {
            $i = intval($value);
            if ($i >= 0) {
                return $i;
            }
        }
        return 0;
    }

    public static function IS_FLOAT_GREATER_EQUAL_ZERO($value)
    {
        if (is_numeric($value)) {
            $f = floatval($value);
            if ($f >= 0) {
                return $f;
            }
        }
        return 0;
    }
}




class CSVReader implements Iterator
{

   const DELIMITER_TAB = "\t";
   const DELIMITER_COMMA = ",";

    private $FileName;
    private $FilePointer;

    private $CurrentLine;
    private $LineCounter = 0;

    private $Header;
    private $RenamedHeader;

    private $ColumnFilters = [];

    private $delimiter;

    public function __construct($file, $delimiter = CSVReader::DELIMITER_TAB)
    {
        $this->delimiter = $delimiter;
        $this->CheckAndReadFile($file);
    }

    private function CheckAndReadFile($file)
    {
        if (is_string($file)) {
            $this->FileName = $file;
            $this->ReadCsvFile();
        } else {
            throw new Exception("Invalid file: " . var_export($file, true));
        }

        //first row should be the header
        $this->next();
        $this->Header = $this->current();
        $this->FilterHeaders();
    }


    private function ReadCsvFile()
    {

        if (!is_file($this->FileName)) {
            throw new Exception(
                "File does not exists: " . $this->FileName
            );
        }
        $this->FilePointer = @fopen($this->FileName, "r");
        if (!$this->FilePointer) {
            throw new Exception(
                "Cannot open file " . $this->FileName . " " . error_get_last()['message']
            );
        }
    }

    private function ReadLine()
    {
        $row = fgetcsv($this->FilePointer, null, $this->delimiter);
        $rowLen = count($row);
        $headerLen = $this->GetColumnsCount();
        return $row;
    }

    public function next()
    {
        $this->CurrentLine = $this->ReadLine();
        $this->LineCounter++;
    }

    public function key()
    {
        return $this->LineCounter;
    }

    public function current()
    {
        return $this->CurrentLine;
    }

    public function valid()
    {
        return $this->CurrentLine != false;
    }

    public function rewind()
    {
        rewind($this->FilePointer);
        $dumpheader = $this->ReadLine();
        $this->CurrentLine = $this->ReadLine();
    }

    public function GetColumnsCount()
    {
        return count($this->getHeader());
    }

    public function GetHeader()
    {
        if ($this->Header) {
            return $this->Header;
        }
        return [];
    }



    public function GetFilteredHeader()
    {
        if ($this->RenamedHeader) {
            return $this->RenamedHeader;
        }
        return [];
    }

    public function FilterHeaders()
    {
        $this->PrepareColumnFilterData();
        $this->RenamedHeader = [];
        foreach ($this->Header as $key => $value) {

            $LocalCol = $value;

            if (array_key_exists($LocalCol, $this->ColumnFilters)) {
                $LocalModifier =  $this->ColumnFilters[$LocalCol];

                if ($LocalModifier !== null && $LocalModifier->IsHeaderNameValid()) {

                    $LocalCol = $LocalModifier->GetHeaderName();
                }

                array_push($this->RenamedHeader, $LocalCol);
            }
        }
    }

    public function FilterRowValues($row)
    {
        $ValidatedValues = [];
        $lenHeader = $this->GetColumnsCount();
        $lenRow = count($row);
        if ($lenHeader != $lenRow) {
            throw new Exception(
                "Csv mismatch Col number exptected: $lenHeader Col number Received: $lenRow on line: $this->LineCounter"
            );
        }
        $MergedHeaderRow = array_combine($this->GetHeader(), $row);
        foreach ($MergedHeaderRow as $key => $value) {
            $ValidValue = $this->ValidateCol($key, $value);
            if ($ValidValue !== NULL) {
                array_push($ValidatedValues, $ValidValue);
            }
        }
        return $ValidatedValues;
    }

    
    public function ValidateCol($RowName, $value)
    {
        if (array_key_exists($RowName, $this->ColumnFilters)) {
            $LocalModifier =  $this->ColumnFilters[$RowName];
            if ($LocalModifier !== null && $LocalModifier->IsValidateFunctionValid()) {
                return $LocalModifier->Validate($value);
            }
            return $value;
        }
        return NULL;
    }


    /**
     * override this function to map column names to dbshema
     */
    protected function PrepareColumnFilterData()
    {
        $this->ColumnFilters =   array(
            "article" =>  new CsvToDBModifier("article", "CsvToDBModifier::IS_INT_ELSE_THROW_ECXEPTION", $this),
            "name" => NULL,
            "image_url" => new CsvToDBModifier("image", null, null),
            "deeplink" => new CsvToDBModifier("url", null, null),
            "price" => new CsvToDBModifier("Price", "CsvToDBModifier::IS_FLOAT_GREATER_EQUAL_ZERO", null),
            "old_price" => new CsvToDBModifier("Price_old", "CsvToDBModifier::IS_FLOAT_GREATER_EQUAL_ZERO", null)
        );
    }
}
