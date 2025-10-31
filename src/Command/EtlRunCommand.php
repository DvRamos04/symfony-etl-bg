<?php
namespace App\Command;

use App\Service\Crypto;
use App\Service\SftpUploader;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(name: "etl:run", description: "Run ETL -> DB -> Encrypt -> SFTP")]
class EtlRunCommand extends Command
{
  private string $storage;

  public function __construct()
  {
    parent::__construct();
    $this->storage = dirname(__DIR__,2)."/storage";
    if (!is_dir($this->storage)) @mkdir($this->storage, 0777, true);
  }

  protected function execute(InputInterface $input, OutputInterface $output): int
  {
    $dotenv = new \Symfony\Component\Dotenv\Dotenv();
    $envPath = dirname(__DIR__,2).'/.env';
    if (is_file($envPath)) { $dotenv->usePutenv(); $dotenv->load($envPath); }
    date_default_timezone_set(getenv('TIMEZONE') ?: 'UTC');

    $pdo = new \PDO(
      sprintf("mysql:host=%s;port=%s;dbname=%s;charset=utf8mb4", getenv('DB_HOST'), getenv('DB_PORT'), getenv('DB_NAME')),
      getenv('DB_USER'), getenv('DB_PASSWORD'),
      [\PDO::ATTR_ERRMODE=>\PDO::ERRMODE_EXCEPTION]
    );
    $this->ensureSchema($pdo);

    $today = (new \DateTime('now'))->format('Ymd');
    $jsonPath = "{$this->storage}/data_{$today}.json";
    $etlCsv   = "{$this->storage}/etl_{$today}.csv";
    $sumCsv   = "{$this->storage}/summary_{$today}.csv";

    // Extract
    $raw = file_get_contents('https://dummyjson.com/users');
    if ($raw===false) throw new \RuntimeException('Fetch failed');
    file_put_contents($jsonPath, $raw);
    $data = json_decode($raw, true);
    $users = $data['users'] ?? [];

    // Transform ? detalle CSV
    $fp = fopen($etlCsv,'w');
    fputcsv($fp, ['id','firstName','lastName','age','gender','city','companyTitle']);
    foreach ($users as $u){
      fputcsv($fp, [
        $u['id']??null, $u['firstName']??'', $u['lastName']??'',
        $u['age']??null, $u['gender']??'', $u['address']['city']??'',
        $u['company']['title']??''
      ]);
    }
    fclose($fp);

    // Resumen (gender/age/os)
    $gender=['male'=>0,'female'=>0,'other'=>0]; $ages=[]; $os=[];
    foreach ($users as $u){
      $g = $u['gender']??'other'; if (!isset($gender[$g])) $g='other'; $gender[$g]++;
      $age=(int)($u['age']??0); $ages[$this->ageBucket($age)] = ($ages[$this->ageBucket($age)]??0)+1;
      $title = $u['company']['title']??'Unknown'; $os[$title] = ($os[$title]??0)+1;
    }
    ksort($ages);

    $fp2 = fopen($sumCsv,'w');
    fputcsv($fp2, ['registre', array_sum($gender)]);
    fputcsv($fp2, ['gender','total']); foreach ($gender as $k=>$v) fputcsv($fp2, [$k,$v]);
    fputcsv($fp2, []); fputcsv($fp2, ['age','total']); foreach ($ages as $k=>$v) fputcsv($fp2, [$k,$v]);
    fputcsv($fp2, []); fputcsv($fp2, ['os','total']);  foreach ($os as $k=>$v) fputcsv($fp2, [$k,$v]); fclose($fp2);

    // Load DB
    $pdo->beginTransaction();
    $pdo->prepare("INSERT INTO etl_execution(run_date) VALUES (NOW())")->execute();
    $execId = (int)$pdo->lastInsertId();

    if (($h=fopen($etlCsv,'r'))!==false){
      fgetcsv($h);
      while(($row=fgetcsv($h))!==false){
        [$id,$fn,$ln,$age,$g,$city,$title] = $row;
        $pdo->prepare("INSERT INTO etl_detail(execution_id, user_id, first_name, last_name, age, gender, city, os) VALUES(?,?,?,?,?,?,?,?)")
            ->execute([$execId,$id,$fn,$ln,$age,$g,$city,$title]);
      }
      fclose($h);
    }

    foreach ($gender as $k=>$v){
      $pdo->prepare("INSERT INTO etl_summary(execution_id, section, k, total, male, female, other) VALUES(?,?,?,?,?,?,?)")
          ->execute([$execId,'gender',$k,$v, ($k==='male'?$v:0), ($k==='female'?$v:0), ($k==='other'?$v:0)]);
    }
    foreach ($ages as $k=>$v){
      $pdo->prepare("INSERT INTO etl_summary(execution_id, section, k, total, male, female, other) VALUES(?,?,?,?,0,0,0)")
          ->execute([$execId,'age',$k,$v]);
    }
    foreach ($os as $k=>$v){
      $pdo->prepare("INSERT INTO etl_summary(execution_id, section, k, total, male, female, other) VALUES(?,?,?,?,0,0,0)")
          ->execute([$execId,'os',$k,$v]);
    }
    $pdo->commit();

    // Encrypt
    $crypto = new Crypto();
    $enc1 = $jsonPath.'.enc';  $crypto->encryptFile($jsonPath,$enc1);
    $enc2 = $etlCsv.'.enc';    $crypto->encryptFile($etlCsv,$enc2);
    $enc3 = $sumCsv.'.enc';    $crypto->encryptFile($sumCsv,$enc3);

    // SFTP (si habilitado)
    $uploaded = [];
    if ((getenv('SFTP_ENABLE') ?: '0') === '1') {
      try {
        $uploader = new SftpUploader();
        $uploader->upload([$enc1,$enc2,$enc3]);
        $uploaded = [$enc1,$enc2,$enc3];
      } catch (\Throwable $e) {
        // Mostrar error pero no romper el resto del ETL
        $uploaded = ['ERROR: '.$e->getMessage()];
      }
    }

    $output->writeln(sprintf(
      "OK ExecID=%d`nFiles:`n- %s`n- %s`n- %s`nEncrypted:`n- %s`n- %s`n- %s`nSFTP: %s",
      $execId, $jsonPath, $etlCsv, $sumCsv, $enc1, $enc2, $enc3,
      empty($uploaded)?'disabled':implode(', ',$uploaded)
    ));
    return Command::SUCCESS;
  }

  private function ensureSchema(\PDO $pdo): void
  {
    $pdo->exec("CREATE TABLE IF NOT EXISTS etl_execution(
      id INT AUTO_INCREMENT PRIMARY KEY,
      run_date DATETIME NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

    $pdo->exec("CREATE TABLE IF NOT EXISTS etl_detail(
      id INT AUTO_INCREMENT PRIMARY KEY,
      execution_id INT NOT NULL,
      user_id INT NULL,
      first_name VARCHAR(100), last_name VARCHAR(100),
      age INT NULL, gender VARCHAR(20), city VARCHAR(100), os VARCHAR(150),
      FOREIGN KEY (execution_id) REFERENCES etl_execution(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

    $pdo->exec("CREATE TABLE IF NOT EXISTS etl_summary(
      id INT AUTO_INCREMENT PRIMARY KEY,
      execution_id INT NOT NULL,
      section VARCHAR(32) NOT NULL,
      k VARCHAR(128) NOT NULL,
      total INT NOT NULL,
      male INT NULL, female INT NULL, other INT NULL,
      FOREIGN KEY (execution_id) REFERENCES etl_execution(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
  }

  private function ageBucket(int $age): string
  {
    $ranges = [[0,10],[11,20],[21,30],[31,40],[41,50],[51,60],[61,70],[71,80],[81,90],[91,999]];
    foreach ($ranges as [$a,$b]) {
      if ($age >= $a && $age <= $b) return ($b==999)?'91+':sprintf('%d-%d',$a,$b);
    }
    return 'unknown';
  }
}
