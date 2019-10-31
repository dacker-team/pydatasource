#!/usr/bin/env bash
#sed  -i -e "sx/$1/$2/g" setup.py
#rm setup.py-e

git add .
git ci -m "$1"
git push

rm -r dist
rm -r build
python setup.py sdist bdist_wheel
twine upload  dist/*