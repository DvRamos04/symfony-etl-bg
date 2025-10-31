<?php
namespace App\Service;

use phpseclib3\Net\SFTP;
use phpseclib3\Crypt\PublicKeyLoader;

class SftpUploader {
  private string $host;
  private int    $port;
  private string $user;
  private ?string $pass;
  private ?string $keyPath;
  private ?string $keyPass;
  private string $remoteDir;

  public function __construct()
  {
    $this->host     = getenv('SFTP_HOST') ?: '127.0.0.1';
    $this->port     = (int)(getenv('SFTP_PORT') ?: 22);
    $this->user     = getenv('SFTP_USER') ?: 'user';
    $this->pass     = getenv('SFTP_PASS') ?: null;
    $this->keyPath  = getenv('SFTP_PRIVATE_KEY_PATH') ?: null;
    $this->keyPass  = getenv('SFTP_PRIVATE_KEY_PASSPHRASE') ?: null;
    $this->remoteDir= getenv('SFTP_REMOTE_DIR') ?: '/upload';
  }

  public function upload(array $files): void
  {
    $sftp = new SFTP($this->host, $this->port, 15);
    $ok = false;

    if ($this->keyPath && is_file($this->keyPath)) {
      $key = PublicKeyLoader::loadPrivateKey(file_get_contents($this->keyPath), $this->keyPass ?: null);
      $ok = $sftp->login($this->user, $key);
    } elseif ($this->pass !== null && $this->pass !== '') {
      $ok = $sftp->login($this->user, $this->pass);
    } else {
      throw new \RuntimeException('SFTP credentials not provided');
    }

    if (!$ok) throw new \RuntimeException('SFTP login failed');

    $this->ensureDir($sftp, $this->remoteDir);

    foreach ($files as $local) {
      $base = basename($local);
      $remote = rtrim($this->remoteDir,'/').'/'.$base;
      if (!$sftp->put($remote, file_get_contents($local))) {
        throw new \RuntimeException('SFTP put failed: '.$remote);
      }
    }
  }

  private function ensureDir(SFTP $sftp, string $path): void
  {
    $parts = array_filter(explode('/', $path));
    $cur = '';
    foreach ($parts as $p){
      $cur .= '/'.$p;
      if (!$sftp->is_dir($cur)) { $sftp->mkdir($cur); }
    }
  }
}
