if [ -z "$1" ]
  then
    date="2018-01-{01..31}-{0..23}"
  else
    date=$1
fi


curl -s -o ./data/gharchive.json.gz https://data.gharchive.org/${date}.json.gz