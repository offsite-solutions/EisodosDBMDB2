<?php /** @noinspection DuplicatedCode SpellCheckingInspection PhpUnusedFunctionInspection NotOptimalIfConditionsInspection */
  
  namespace Eisodos\Connectors;
  
  require_once("MDB2.php");
  
  use Eisodos\Eisodos;
  use Eisodos\Interfaces\DBConnectorInterface;
  use MDB2;
  use MDB2_Driver_Common;
  use PEAR;
  use RuntimeException;
  
  /**
   * Eisodos MDB2 Connector class
   *
   */
  class ConnectorMDB2 implements DBConnectorInterface {
    
    /** @var MDB2_Driver_Common null */
    private MDB2_Driver_Common $connection;
    
    /** @var array */
    private $lastQueryColumnNames = [];
    
    /** @var int */
    private $lastQueryTotalRows = 0;
    
    public function connected(): bool {
      return (isset($this->connection) && !($this->connection === NULL));
    }
    
    /**
     * @inheritDoc
     * throws RuntimeException
     */
    public function connect($databaseConfigSection_ = 'Database', $connectParameters_ = [], $persistent_ = false): void {
      if (!isset($this->connection)) {
        // loading connect string
        $databaseConfig = array_change_key_case(Eisodos::$configLoader->importConfigSection($databaseConfigSection_, '', false), CASE_LOWER);
        parse_str(Eisodos::$utils->safe_array_value($databaseConfig, "options", ""), $databaseOptions);
        $connection = MDB2::connect($databaseConfig["login"], $databaseOptions);
        
        if (PEAR::isError($connection)) {
          Eisodos::$logger->writeErrorLog(NULL, $connection->getMessage() . "\n" .
            $connection->getUserInfo() . "\n" .
            $connection->getDebugInfo());
          Eisodos::$parameterHandler->setParam("DBError", $connection->getMessage() . "\n" .
            $connection->getUserInfo() . "\n" .
            $connection->getDebugInfo());
          throw new RuntimeException("Database Open Error!");
        }
        
        $this->connection = $connection;
        
        Eisodos::$logger->trace("Database connected - " . $this->connection->connected_database_name);
        
        if (($connectSQL = Eisodos::$utils->safe_array_value($databaseConfig, "connectsql", "")) !== "") {
          $a = explode(';', $connectSQL);
          foreach ($a as $sql) {
            if ($sql !== '') {
              $this->query(RT_FIRST_ROW_FIRST_COLUMN, $sql);
            }
          }
        }
        
      }
    }
    
    /** @inheritDoc */
    public function query(
      int $resultTransformation_, string $SQL_, &$queryResult_ = NULL, $getOptions_ = [], $exceptionMessage_ = ''
    ) {
      
      $this->lastQueryColumnNames = [];
      $this->lastQueryTotalRows = 0;
      
      if (!isset($this->connection)) {
        throw new RuntimeException("Database not connected");
      }
      
      $resultSet = $this->connection->query($SQL_);
      if (PEAR::isError($resultSet)) {
        if (!$exceptionMessage_) {
          $_POST["__EISODOS_extendedError"] = $resultSet->getUserInfo();
          throw new RuntimeException($resultSet->getMessage());
        }
        
        $resultSet['error'] = $resultSet->getMessage();
        
        return false;
        
      }
      
      $this->lastQueryColumnNames = $resultSet->getColumnNames();
      $this->lastQueryTotalRows = $resultSet->numRows();
      
      if ($resultTransformation_ === RT_RAW) {
        $rows = $resultSet->fetchAll(MDB2_FETCHMODE_ASSOC);
        if (!$rows) {
          $resultSet->free();
          
          return false;
        }
        
        $queryResult_ = $rows;
        $resultSet->free();
        
        return true;
      }
      
      if ($resultTransformation_ === RT_FIRST_ROW) {
        $row = $resultSet->fetchRow(MDB2_FETCHMODE_ASSOC);
        if (!$row) {
          $resultSet->free();
          
          return false;
        }
        
        $queryResult_ = $row;
        $resultSet->free();
        
        return true;
      }
      
      if ($resultTransformation_ === RT_FIRST_ROW_FIRST_COLUMN) {
        $resultSet = $this->connection->query($SQL_);
        if (PEAR::isError($resultSet)) {
          $_POST["__EISODOS_extendedError"] = $resultSet->getUserInfo();
          throw new RuntimeException($resultSet->getMessage());
        }
        
        $row = $resultSet->fetchRow(MDB2_FETCHMODE_DEFAULT);
        if (!$row) {
          $resultSet->free();
          
          return '';
        }
        
        $back = $row[0];
        $resultSet->free();
        
        return $back;
      }
      
      if ($resultTransformation_ === RT_ALL_KEY_VALUE_PAIRS
        or $resultTransformation_ === RT_ALL_FIRST_COLUMN_VALUES
        or $resultTransformation_ === RT_ALL_ROWS
        or $resultTransformation_ === RT_ALL_ROWS_ASSOC) {
        
        $resultSet = $this->connection->query($SQL_);
        if (PEAR::isError($resultSet)) {
          if (!$exceptionMessage_) {
            $_POST["__EISODOS_extendedError"] = $resultSet->getUserInfo();
            throw new RuntimeException($resultSet->getMessage());
          }
          
          $queryResult_['error'] = $resultSet->getMessage();
          
          return false;
        }
        
        // TODO okosabban, gyorsabban
        if ($resultTransformation_ === RT_ALL_KEY_VALUE_PAIRS) {
          while (($row = $resultSet->fetchRow(MDB2_FETCHMODE_ORDERED))) {
            $queryResult_[$row[0]] = $row[1];
          }
        } else if ($resultTransformation_ === RT_ALL_FIRST_COLUMN_VALUES) {
          while (($row = $resultSet->fetchRow(MDB2_FETCHMODE_ORDERED))) {
            $queryResult_[] = $row[0];
          }
        } else if ($resultTransformation_ === RT_ALL_ROWS) {
          while (($row = $resultSet->fetchRow(MDB2_FETCHMODE_ASSOC))) {
            $queryResult_[] = $row;
          }
        } else if ($resultTransformation_ === RT_ALL_ROWS_ASSOC) {
          $indexFieldName = Eisodos::$utils->safe_array_value($getOptions_, 'indexFieldName', false);
          if (!$indexFieldName) {
            throw new RuntimeException("Index field name is mandatory on RT_ALL_ROWS_ASSOC result type");
          }
          while (($row = $resultSet->fetchRow(MDB2_FETCHMODE_ASSOC))) {
            $queryResult_[$row[$indexFieldName]] = $row;
          }
        }
        
        $resultSet->free();
        
        return true;
      }
      
      throw new RuntimeException("Unknown query result type");
      
    }
    
    /** @inheritDoc */
    public function getLastQueryColumns(): array {
      return $this->lastQueryColumnNames;
    }
    
    /** @inheritDoc */
    public function getLastQueryTotalRows(): int {
      return $this->lastQueryTotalRows;
    }
    
    /** @inheritDoc */
    public function disconnect($force_ = false): void {
      if (isset($this->connection)) {
        $this->connection->disconnect($force_);
      }
    }
    
    /** @inheritDoc */
    public function startTransaction($savePoint_ = NULL): void {
      if (!isset($this->connection)) {
        throw new RuntimeException("Database not connected");
      }
      $this->connection->beginTransaction($savePoint_);
    }
    
    /** @inheritDoc */
    public function commit(): void {
      if (!isset($this->connection)) {
        throw new RuntimeException("Database not connected");
      }
      $this->connection->commit();
    }
    
    /** @inheritDoc */
    public function rollback($savePoint_ = NULL): void {
      if (!isset($this->connection)) {
        throw new RuntimeException("Database not connected");
      }
      $this->connection->rollback($savePoint_);
    }
    
    /** @inheritDoc */
    public function inTransaction(): bool {
      if (!isset($this->connection)) {
        throw new RuntimeException("Database not connected");
      }
      
      $inTransaction = $this->connection->inTransaction();
      
      return ($inTransaction ?? false);
    }
  
    public function executeDML(string $SQL_, $throwException_ = true): int {
      if (!isset($this->connection)) {
        throw new RuntimeException("Database not connected");
      }
      $affectedRows = $this->connection->exec($SQL_);
      if (PEAR::isError($affectedRows)) {
        if ($throwException_) {
          $_POST["__EISODOS_extendedError"] = $affectedRows->getUserInfo();
          throw new RuntimeException($affectedRows->getMessage());
        }
        
        return 0;
      }
      
      return $affectedRows;
    }
  
    public function executePreparedDML(string $SQL_, $dataTypes_ = [], $data_ = [], $throwException_ = true): int {
      if (!isset($this->connection)) {
        throw new RuntimeException("Database not connected");
      }
      $sth = $this->connection->prepare($SQL_, $dataTypes_, MDB2_PREPARE_MANIP);
      if (PEAR::isError($sth)) {
        if ($throwException_) {
          $_POST["__EISODOS_extendedError"] = $sth->getUserInfo();
          throw new RuntimeException($sth->getMessage());
        }
        
        return $sth->getMessage();
      }
      
      $affectedRows = $sth->execute($data_);
      if (PEAR::isError($affectedRows)) {
        if ($throwException_) {
          $_POST["__EISODOS_extendedError"] = $affectedRows->getUserInfo();
          throw new RuntimeException($affectedRows->getMessage());
        }
        
        return 0;
      }
      
      $sth->free();
      
      return $affectedRows;
    }
    
    /** @inheritDoc */
    public function bind(array &$boundVariables_, string $variableName_, string $dataType_, string $value_, $inOut_ = 'IN') {
      $boundVariables_[$variableName_] = array();
      if ($dataType_ === "clob" && $value_ === '') // Empty CLOB bug / invalid LOB locator specified, force type to text
      {
        $boundVariables_[$variableName_]["type"] = "text";
      } else {
        $boundVariables_[$variableName_]["type"] = $dataType_;
      }
      $boundVariables_[$variableName_]["value"] = $value_;
      $boundVariables_[$variableName_]["mode_"] = $inOut_;
    }
    
    /** @inheritDoc */
    public function bindParam(array &$boundVariables_, string $parameterName_, string $dataType_) {
      $this->bind($boundVariables_, $parameterName_, $dataType_, Eisodos::$parameterHandler->getParam($parameterName_));
    }
    
    /** @inheritDoc */
    public function executeStoredProcedure(string $procedureName_, array $inputVariables_, array &$resultVariables_, $throwException_ = true, $case_ = CASE_UPPER): bool {
      if (!isset($this->connection)) {
        throw new RuntimeException("Database not connected");
      }
    
      $sql = "";
      if ($this->connection->dbsyntax === 'oci8') {
        foreach ($inputVariables_ as $parameterName => $parameterProperties)
          $sql .= ($sql ? "," : "") . $parameterName . " => :" . $parameterName;
        $sql = "begin " . $procedureName_ . "(" . $sql . "); end; ";
      } else if ($this->connection->dbsyntax === 'pgsql') {
        foreach ($inputVariables_ as $parameterName => $parameterProperties) {
          if ($parameterProperties["mode_"] !== "OUT") // skip OUT parameters
            $sql .= ($sql ? "," : "") . $parameterName . " => :" . $parameterName;
        }
        $sql = "select * from " . $procedureName_ . "(" . $sql . ")";
      }
      
      $sth = $this->connection->prepare($sql);
      if (PEAR::isError($sth)) {
        if ($throwException_) {
          $_POST["__EISODOS_extendedError"] = $sth->getUserInfo();
          throw new RuntimeException($sth->getMessage());
        }
        
        return false;
      }
      
      foreach ($inputVariables_ as $paramName => $parameterProperties) {
        $resultVariables_[$paramName] = $parameterProperties["value"];
        $sth->bindParam($paramName,
          $resultVariables_[$paramName],
          $parameterProperties["type"],
          (($parameterProperties["type"] === "integer" || $parameterProperties["type"] === "text") ? (32766 / 2) : -1)
        );
      }
      $rs =& $sth->execute();
      if (PEAR::isError($rs)) {
        $_POST["__udSCGI_extendedError"] = $rs->getUserInfo() . "\nBinded variables: " . print_r($inputVariables_, true);
        $sth->free();
        throw new RuntimeException($rs->getMessage());
      }
      
      if ($this->connection->dbsyntax === 'pgsql') {
        $result = $rs->fetchRow(MDB2_FETCHMODE_ASSOC);
        if (is_array($result)) {
          $resultVariables_ = array_merge($resultVariables_, array_change_key_case($result, $case_));
        }
      }
      $rs->free();
      $sth->free();
      
      return true;
    }
    
    /**
     * @inheritDoc
     */
    public function getConnection() {
      return $this->connection;
    }
    
    /**
     * @inheritDoc
     */
    public function nullStr($value_, $isString_ = true, $maxLength_ = 0, $exception_ = "", $withComma_ = false): string {
      return $this->emptySQLField($value_, $isString_, $maxLength_, $exception_, $withComma_, "NULL");
    }
    
    /**
     * @inheritDoc
     */
    public function emptySQLField($value_, $isString_ = true, $maxLength_ = 0, $exception_ = "", $withComma_ = false, $keyword_ = "NULL"): string {
      if ($value_ === '') {
        if ($withComma_) {
          return "NULL, ";
        }
        
        return "NULL";
      }
      if ($isString_) {
        if ($maxLength_ > 0 && mb_strlen($value_, 'UTF-8') > $maxLength_) {
          if ($exception_) {
            throw new RuntimeException($exception_);
          }
        
          $value_ = substr($value_, 0, $maxLength_);
        }
        $result = "'" . Eisodos::$utils->replace_all($value_, "'", "''") . "'";
      } else {
        $result = $value_;
      }
      if ($withComma_) {
        $result .= ", ";
      }
      
      return $result;
    }
    
    /**
     * @inheritDoc
     */
    public function defaultStr($value_, $isString_ = true, $maxLength_ = 0, $exception_ = "", $withComma_ = false): string {
      return $this->emptySQLField($value_, $isString_, $maxLength_, $exception_, $withComma_, "DEFAULT");
    }
    
    /**
     * @inheritDoc
     */
    public function nullStrParam(string $parameterName_, $isString_ = true, $maxLength_ = 0, $exception_ = "", $withComma_ = false): string {
      return $this->emptySQLField(Eisodos::$parameterHandler->getParam($parameterName_), $isString_, $maxLength_, $exception_, $withComma_, "NULL");
    }
    
    /**
     * @inheritDoc
     */
    public function defaultStrParam(string $parameterName_, $isString_ = true, $maxLength_ = 0, $exception_ = "", $withComma_ = false): string {
      return $this->emptySQLField(Eisodos::$parameterHandler->getParam($parameterName_), $isString_, $maxLength_, $exception_, $withComma_, "DEFAULT");
    }
    
  }