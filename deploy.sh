#!/usr/bin/env bash
sed  -i -e "s/$1/$2/g" setup.py
rm setup.py-e

git add .
git ci -m "$3"
git push

rm -r dist
rm -r build
python setup.py sdist bdist_wheel
twine upload  dist/*