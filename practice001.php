<?php
require_once(dirname(__FILE__)."/Database.php");

ini_set('max_execution_time', 600); // 연결시간

class Practice001 {
  private $mysqli_conn;
  private $db;
  public function __construct() {
    $this->db = new Database("localhost", "root", "", "rdbms_practice");
    $this->mysqli_conn = $this->db->getMysqliConnection();
  }

  private function makePracticeTable() {
    $sql = "
    CREATE TABLE IF NOT EXISTS `practice001` (
        `idx` int unsigned NOT NULL AUTO_INCREMENT,
        `column_001` varchar(20) NOT NULL,
        `column_002` varchar(20) NOT NULL,
        `column_003` varchar(20) NOT NULL,
        `column_004` varchar(20) NOT NULL,
        `column_005` varchar(20) NOT NULL,
        `column_006` varchar(20) NOT NULL,
        `column_007` varchar(20) NOT NULL,
        `column_008` varchar(20) NOT NULL,
        `column_009` varchar(20) NOT NULL,
        `create_date` datetime DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (`idx`),
        UNIQUE KEY `unique1` (`column_001`,`column_002`,`column_003`)
      ) ENGINE=MyISAM AUTO_INCREMENT=200001 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
      ";
    return $this->db->getPdoConnection()->prepare($sql)->execute();
  }

  private function makeData(int $size = 100_000) : array {
    
    $data = [];
    for($i=0; $i< $size; $i++) {
        $data[$i] = [
            "column_001" => "value_" . rand(00000, 99999),
            "column_002" => "value_" . rand(00000, 99999),
            "column_003" => "value_" . rand(00000, 99999),
            "column_004" => "value_" . rand(00000, 99999),
            "column_005" => "value_" . rand(00000, 99999),
            "column_006" => "value_" . rand(00000, 99999),
            "column_007" => "value_" . rand(00000, 99999),
            "column_008" => "value_" . rand(00000, 99999),
            "column_009" => "value_" . rand(00000, 99999)
        ];
    }
    return $data;
  }

  /**
   * 하나씩 등록한다
   * 약 16초
   *
   * @return array
   */
  public function insertPractice001() : array {
    $start_time = microtime(true);
    $cnt=0;
   
    $data = $this->makeData();

    foreach($data as $row) {
        $cnt += $this->db->insertData($row, "practice001");
    }

    $end_time = microtime(true);
    $time_diff = $end_time - $start_time;

    $result = [
        'cnt' => $cnt,
        'time' => "Insertion completed in ".round($time_diff,2)." seconds"
    ];
    return $result;
  }
  /**
   * Multipart 로 insert한다
   * 약 3.5 초
   * @return array
   */
  public function insertPractice001_bath() : array{

    $start_time = microtime(true);
    $cnt=0;
   
    $data = $this->makeData();

    $cnt += $this->db->insertData_bath($data, "practice001");

    $end_time = microtime(true);
    $time_diff = $end_time - $start_time;

    $result = [
        'cnt' => $cnt,
        'time' => "Insertion completed in ".round($time_diff,2)." seconds"
    ];
    return $result;

  }

  public function insertPractice001_transaction1() {
    $start_time = microtime(true);
    $cnt=0;
   
    $data = $this->makeData();

    try {
        $cnt += $this->db->insertData_trans1($data, "practice001");
        $end_time = microtime(true);
        $time_diff = $end_time - $start_time;
        $result = [
            'cnt' => $cnt,
            'time' => "Insertion completed in ".round($time_diff,2)." seconds"
        ];
    } catch (PDOException $e) {
        $this->db->rollback();
        $result = [
            'cnt' => 0,
            'time' => "Insertion failed with error: " . $e->getMessage()
        ];
    }
    return $result;

  }

  public function insertPractice001_transaction2() {
    $start_time = microtime(true);
    $cnt=0;
   
    $data = $this->makeData();

    try {
        $cnt += $this->db->insertData_trans2($data, "practice001");
        $end_time = microtime(true);
        $time_diff = $end_time - $start_time;
        $result = [
            'cnt' => $cnt,
            'time' => "Insertion completed in ".round($time_diff,2)." seconds"
        ];
    } catch (PDOException $e) {
        $this->db->rollback();
        $result = [
            'cnt' => 0,
            'time' => "Insertion failed with error: " . $e->getMessage()
        ];
    }
    return $result;

  }

  public function closeConnection() {
    $this->mysqli_conn->close();
  }
}

$prac = new Practice001();


//기존 insert
$result1 = $prac->insertPractice001();
echo json_encode($result1);

// echo "<br>";

// //다중 insert
// $result2 = $prac->insertPractice001_bath();
// echo json_encode($result2);

// echo "<br>";

// //트랜잭션 insert
// $result3 = $prac->insertPractice001_transaction1();
// echo json_encode($result3);

// //트랜잭션 다중 insert
// $result4 = $prac->insertPractice001_transaction2();
// echo json_encode($result4);


$prac->closeConnection();


?>
