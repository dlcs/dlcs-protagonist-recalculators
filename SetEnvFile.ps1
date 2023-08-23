Get-Content .env | foreach {
  $name, $value = $_.split('=')
  if ([string]::IsNullOrWhiteSpace($name) -or $name.Contains('#')) {
    echo "$name is null or disabled"
  } else{
      echo "env:$name $value"
      Set-Content env:\$name $value
  }
}