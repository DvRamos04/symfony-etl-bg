<?php
namespace App\Command;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\HttpClient\HttpClient;

#[AsCommand(name: "etl:run", description: "Descarga JSON, genera CSVs, inserta en MySQL y cifra archivos")]
class RunEtlCommand extends Command {
  private \PDO $pdo; private string $storage; private string $apiBase; private string $key; private string $iv;

  protected function initialize(InputInterface $in, OutputInterface $out): void {
    $dotenv = new \Symfony\Component\Dotenv\Dotenv();
    $envPath = dirname(__DIR__,2)."/.env"; if (is_file($envPath)) { $dotenv->usePutenv(); $dotenv->load($envPath); }
    date_default_timezone_set(getenv("TIMEZONE") ?: "UTC");

    $host = getenv("DB_HOST") ?: "127.0.0.1";
    $port = getenv("DB_PORT") ?: "3306";
    $name = getenv("DB_NAME") ?: "etl_backend";
    $user = getenv("DB_USER") ?: "root";
    $pass = getenv("DB_PASSWORD") ?: "";

    // 1) Intentar conectar a la BD; si no existe, crearla
    try {
      $this->pdo = new \PDO("mysql:host=$host;port=$port;dbname=$name;charset=utf8mb4", $user, $pass, [\PDO::ATTR_ERRMODE=>\PDO::ERRMODE_EXCEPTION]);
    } catch (\PDOException $e) {
      if (strpos($e->getMessage(), "Unknown database") !== false || $e->getCode() == 1049) {
        $tmp = new \PDO("mysql:host=$host;port=$port;charset=utf8mb4", $user, $pass, [\PDO::ATTR_ERRMODE=>\PDO::ERRMODE_EXCEPTION]);
        $tmp->exec("CREATE DATABASE IF NOT EXISTS `$name` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;");
        $this->pdo = new \PDO("mysql:host=$host;port=$port;dbname=$name;charset=utf8mb4", $user, $pass, [\PDO::ATTR_ERRMODE=>\PDO::ERRMODE_EXCEPTION]);
      } else { throw $e; }
    }

    // 2) Autocrear tablas si no existen
    $schema = <<<SQL
CREATE TABLE IF NOT EXISTS etl_execution (
  id INT AUTO_INCREMENT PRIMARY KEY,
  run_date DATETIME NOT NULL,
  raw_json_file VARCHAR(255) NOT NULL,
  etl_csv_file VARCHAR(255) NOT NULL,
  summary_csv_file VARCHAR(255) NOT NULL,
  inserted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS etl_detail (
  id INT AUTO_INCREMENT PRIMARY KEY,
  execution_id INT NOT NULL,
  user_id INT,
  gender VARCHAR(16),
  age INT,
  city VARCHAR(100),
  os VARCHAR(50),
  inserted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (execution_id) REFERENCES etl_execution(id)
);
CREATE TABLE IF NOT EXISTS etl_summary (
  id INT AUTO_INCREMENT PRIMARY KEY,
  execution_id INT NOT NULL,
  section VARCHAR(50) NOT NULL,
  k VARCHAR(100) NOT NULL,
  male INT DEFAULT 0,
  female INT DEFAULT 0,
  other INT DEFAULT 0,
  total INT DEFAULT 0,
  inserted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (execution_id) REFERENCES etl_execution(id)
);
SQL;
    $this->pdo->exec($schema);

    $this->storage = str_replace("%kernel.project_dir%", dirname(__DIR__,2), getenv("STORAGE_PATH") ?: (dirname(__DIR__,2)."/storage"));
    @mkdir($this->storage, 0775, true);

    $this->apiBase = rtrim(getenv("API_BASE") ?: "https://dummyjson.com", "/");
    $this->key = getenv("CRYPTO_KEY") ?: "CHANGE_ME_32CHARS_MINIMUM";
    $this->iv  = getenv("CRYPTO_IV")  ?: "1234567890123456";
  }

  protected function execute(InputInterface $in, OutputInterface $out): int {
    $today = date("Ymd");
    $jsonFile = $this->storage."/data_{$today}.json";
    $etlFile  = $this->storage."/etl_{$today}.csv";
    $sumFile  = $this->storage."/summary_{$today}.csv";

    // 1) Descargar JSON
    $http = HttpClient::create();
    $resp = $http->request("GET", $this->apiBase."/users");
    if (200 !== $resp->getStatusCode()) { $out->writeln("<error>API error</error>"); return Command::FAILURE; }
    $json = $resp->getContent();
    file_put_contents($jsonFile, $json);
    $users = json_decode($json, true)["users"] ?? [];

    // 2) CSV detalle
    $fp = fopen($etlFile, "w");
    fputcsv($fp, ["user_id","gender","age","city","os"]);
    foreach ($users as $u) {
      $os = $u["company"]["title"] ?? "Unknown";
      fputcsv($fp, [
        $u["id"] ?? null,
        strtolower($u["gender"] ?? "other"),
        $u["age"] ?? null,
        $u["address"]["city"] ?? "Unknown",
        $os
      ]);
    }
    fclose($fp);

    // 3) CSV summary (formato exigido)
    $summary = ["register"=>["total"=>count($users)], "gender"=>["male"=>0,"female"=>0,"other"=>0], "age"=>[], "city"=>[], "os"=>[]];
    $mkAge = function(int $age): string {
      $ranges = [[0,10],[11,20],[21,30],[31,40],[41,50],[51,60],[61,70],[71,80],[81,90]];
      foreach ($ranges as [$a,$b]) if ($age >= $a && $age <= $b) return "$a-$b";
      return "91+";
    };
    foreach($users as $u){
      $g = strtolower($u["gender"] ?? "other"); if(!in_array($g,["male","female","other"])) $g="other";
      $summary["gender"][$g]++;
      $ageKey = $mkAge((int)($u["age"] ?? 0));
      $summary["age"][$ageKey] = $summary["age"][$ageKey] ?? ["male"=>0,"female"=>0,"other"=>0];
      $summary["age"][$ageKey][$g]++;
      $city = $u["address"]["city"] ?? "Unknown";
      $summary["city"][$city] = $summary["city"][$city] ?? ["male"=>0,"female"=>0,"other"=>0];
      $summary["city"][$city][$g]++;
      $os = $u["company"]["title"] ?? "Unknown";
      $summary["os"][$os] = ($summary["os"][$os] ?? 0) + 1;
    }
    $fp = fopen($sumFile,"w");
    fputcsv($fp, ["registre", $summary["register"]["total"]]);
    fputcsv($fp, ["gender","total"]);
    foreach(["male","female","other"] as $g) fputcsv($fp, [$g, $summary["gender"][$g]]);
    fputcsv($fp, []); fputcsv($fp, ["age","male","female","other"]);
    foreach($summary["age"] as $range=>$row) fputcsv($fp, [$range,$row["male"],$row["female"],$row["other"]]);
    fputcsv($fp, []); fputcsv($fp, ["City","male","female","other"]);
    foreach($summary["city"] as $city=>$row) fputcsv($fp, [$city,$row["male"],$row["female"],$row["other"]]);
    fputcsv($fp, []); fputcsv($fp, ["SO","total"]);
    foreach($summary["os"] as $os=>$tot) fputcsv($fp, [$os,$tot]);
    fclose($fp);

    // 4) Persistencia
    $this->pdo->beginTransaction();
    $stmt = $this->pdo->prepare("INSERT INTO etl_execution(run_date,raw_json_file,etl_csv_file,summary_csv_file) VALUES (NOW(),?,?,?)");
    $stmt->execute([basename($jsonFile), basename($etlFile), basename($sumFile)]);
    $execId = (int)$this->pdo->lastInsertId();

    $ins = $this->pdo->prepare("INSERT INTO etl_detail(execution_id,user_id,gender,age,city,os) VALUES (?,?,?,?,?,?)");
    if (($f = fopen($etlFile,"r"))!==false){ $first=true; while(($row=fgetcsv($f))!==false){ if($first){$first=false;continue;} $ins->execute([$execId,$row[0],$row[1],$row[2],$row[3],$row[4]]);} fclose($f); }

    $insS=$this->pdo->prepare("INSERT INTO etl_summary(execution_id,section,k,male,female,other,total) VALUES (?,?,?,?,?,?,?)");
    foreach(["male","female","other"] as $g){ $insS->execute([$execId,"gender",$g,$g==="male"?$summary["gender"]["male"]:0,$g==="female"?$summary["gender"]["female"]:0,$g==="other"?$summary["gender"]["other"]:0,$summary["gender"][$g]]); }
    foreach($summary["age"] as $k=>$r){ $insS->execute([$execId,"age",$k,$r["male"],$r["female"],$r["other"],$r["male"]+$r["female"]+$r["other"]]); }
    foreach($summary["city"] as $k=>$r){ $insS->execute([$execId,"city",$k,$r["male"],$r["female"],$r["other"],$r["male"]+$r["female"]+$r["other"]]); }
    $insSO=$this->pdo->prepare("INSERT INTO etl_summary(execution_id,section,k,total) VALUES (?,?,?,?)");
    foreach($summary["os"] as $k=>$t){ $insSO->execute([$execId,"os",$k,$t]); }
    $insSO->execute([$execId,"register","total",$summary["register"]["total"]]);
    $this->pdo->commit();

    // 5) Cifrado .enc
    foreach([$jsonFile,$etlFile,$sumFile] as $file){
      $plain = file_get_contents($file);
      $cipher = openssl_encrypt($plain,"aes-256-cbc",$this->key,0,$this->iv);
      file_put_contents($file.".enc", $cipher);
    }

    $out->writeln("<info>OK</info> ExecID=$execId | JSON=$jsonFile | ETL=$etlFile | SUM=$sumFile");
    return Command::SUCCESS;
  }
}
