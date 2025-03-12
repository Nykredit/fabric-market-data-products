#!/bin/bash

# This script creates symbolic links for SparkJobDefinition items.
# It links the 'Libs' and 'Main' directories from the 'fabric' directory
# to the corresponding directories in the 'testing' directory.

# Loop through all SparkJobDefinition fabric items.
for dir in $(find fabric -type d -name "*.SparkJobDefinition"); do
    base_dir=$(basename "$dir") # Name without fabric prefix or any suffix
    
    # Link fabric Libs to testing Libs
    if [ -d "$PWD/$dir/Libs" ] && [ -d "$PWD/testing/$base_dir/Libs" ]; then
    for file in "$PWD/$dir/Libs"/*; do
        ln -s "$file" "$PWD/testing/$base_dir/Libs/$(basename "$file")"
    done
    fi

    # Link fabric Libs to testing Main
    if [ -d "$PWD/$dir/Libs" ] && [ -d "$PWD/testing/$base_dir/Main" ]; then
    for file in "$PWD/$dir/Libs"/*; do
        ln -s "$file" "$PWD/testing/$base_dir/Main/$(basename "$file")"
    done
    fi

    # Link fabric Main to testing Main
    if [ -d "$PWD/$dir/Main" ] && [ -d "$PWD/testing/$base_dir/Main" ]; then
    for file in "$PWD/$dir/Main"/*; do
        ln -s "$file" "$PWD/testing/$base_dir/Main/$(basename "$file")"
    done
    fi
done
