<?php
  
  namespace Eisodos\Connectors;
  
  use Eisodos\Interfaces\DBConnectorInterface;
  use Exception;
  
  /**
   * Eisodos MDB2 Connector class
   *
   */
  class ConnectorMDB2 implements DBConnectorInterface {
    
    public function connect($connectParameters_ = [], $persistent_ = false): void {
    }
    
    public function disconnect(): void {
    }
    
    public function startTransaction(): void {
    }
    
    public function commit(): void {
    }
    
    public function rollback(): void {
    }
    
    public function inTransaction(): bool {
      return false;
    }
    
    public function executeDML($SQL_) {
    }
    
    public function executePreparedDML($SQL_, $bindVariables_ = [], $variableTypes_ = []) {
    }
    
    public function executeStoredProcedure($procedureName_, $bindVariables_ = [], $variableTypes_ = []) {
    }
    
    public function query(
      $SQL_,
      $resultTransformation_,
      &$resultSet = NULL,
      $getOptions = [],
      $exceptionMessage_ = ''
    ) {
    }
    
  }