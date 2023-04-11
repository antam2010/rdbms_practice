<?php
class Database
{
  private $servername;
  private $username;
  private $password;
  private $dbname;
  private $conn;

  private $transaction_active = false;


  public function __construct($servername, $username, $password, $dbname)
  {
    $this->servername = $servername;
    $this->username = $username;
    $this->password = $password;
    $this->dbname = $dbname;
    $this->connect();
  }

  private function connect()
  {
    try {
      $dsn = "mysql:host=$this->servername;dbname=$this->dbname;charset=utf8mb4";
      $options = [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_EMULATE_PREPARES => false,
      ];
      $this->conn = new PDO($dsn, $this->username, $this->password, $options);
    } catch (PDOException $e) {
      die("Connection failed: " . $e->getMessage());
    }
  }

  public function getMysqliConnection()
  {
    return new mysqli($this->servername, $this->username, $this->password, $this->dbname);
  }

  public function getPdoConnection()
  {
    return $this->conn;
  }

  public function beginTransaction()
  {
    $this->conn->beginTransaction();
    $this->transaction_active = true;
  }

  public function commit()
  {
    $this->conn->commit();
    $this->transaction_active = false;
  }

  public function rollback()
  {
    if ($this->transaction_active) {
      $this->conn->rollBack();
      $this->transaction_active = false;
    }
  }

  public function query(string $sql)
  {
    return $this->conn->query($sql);
  }

  public function insertData(array $data, string $tablename)
  {
    $columns = implode(", ", array_keys($data));
    $placeholders = implode(", ", array_fill(0, count($data), "?"));
    $values = array_values($data);
    $stmt = $this->conn->prepare("INSERT INTO $tablename ($columns) VALUES ($placeholders)");
    $stmt->execute($values);
    return $stmt->rowCount();
  }
  
  /**
   * 트랜잭션 테스트
   *
   * @param array $data
   * @param string $tablename
   * @return integer
   */
  public function insertData_trans1(array $data, string $tablename): int
  {
    $cnt = 0;
    $this->beginTransaction();
    try {
      foreach ($data as $row) {
        $cnt += $this->insertData($row, $tablename);
      }
      $this->commit();
    } catch (Exception $e) {
      $this->rollback();
      throw $e;
    }
    return $cnt;
  }

  public function insertData_trans2(array $data, string $tablename): int
  {
    $cnt = 0;
    $this->beginTransaction();
    try {
      $cnt = $this->insertData_bath($data, $tablename);
      $this->commit();
    } catch (Exception $e) {
      $this->rollback();
      throw $e;
    }
    return $cnt;
  }

  /**
   * 데이터를 일괄 삽입합니다.
   * 
   * @param array $data 삽입할 데이터 배열입니다. 각 데이터는 연관 배열이어야 하며, 키는 컬럼명, 값은 삽입할 값입니다.
   * @param string $tableName 데이터를 삽입할 테이블명입니다.
   * @param int $batchSize 일괄 삽입할 데이터 개수입니다. 기본값은 1000입니다.
   * @return int 삽입된 데이터의 행 수를 반환합니다.
   * @throws Exception 일괄 삽입에 실패한 경우 발생합니다.
   */
  public function insertData_bath(array $data, string $tablename): int
  {
    // 1. 최대 패킷 크기 설정 (128MB)
    $max_packet_size = 1024 * 1024 * 128; // MB

    // 2. 최대 배치 크기 설정 (5000건)
    $max_rows_per_batch = 5000;

    // 3. 열 이름 설정
    $columns = implode(", ", array_keys($data[0]));

    // 4. 데이터 총 건수 및 배치 크기 설정
    $row_count = count($data);
    $batch_size = min($max_rows_per_batch, $row_count);

    // 5. 배치 단위로 데이터 삽입
    for ($i = 0; $i < $row_count; $i += $batch_size) {
      // 6. 배치 크기만큼 데이터 추출
      $batch_data = array_slice($data, $i, $batch_size);

      // 7. 데이터 값을 쿼리 문법에 맞게 변환
      $batch_placeholders = "";
      foreach ($batch_data as $row) {
        $batch_placeholders .= "(" . implode(",", array_map(function ($value) {
          return "'" . addslashes($value) . "'";
        }, $row)) . "),";
      }
      $batch_placeholders = rtrim($batch_placeholders, ",");

      // 8. 배치 쿼리문 생성
      $batch_query = "INSERT INTO $tablename ($columns) VALUES $batch_placeholders";

      // 9. 쿼리문의 길이가 최대 패킷 크기보다 큰 경우, 배치 크기를 줄여서 다시 시도
      if (strlen($batch_query) > $max_packet_size) {
        $batch_size = max(1, floor($batch_size / 2));
        continue;
      }

      try {
        // 10. 배치 쿼리문 실행
        $stmt = $this->conn->prepare($batch_query);
        $stmt->execute();
      } catch (PDOException $e) {
        // 11. 배치 크기를 줄여서 다시 시도
        $batch_size = max(1, floor($batch_size / 2));
        continue;
      }

      // 12. 다음 배치 크기 설정
      $batch_size = min($max_rows_per_batch, $row_count - $i - $batch_size);
    }

    // 13. 데이터 총 건수 반환
    return $row_count;
  }


}