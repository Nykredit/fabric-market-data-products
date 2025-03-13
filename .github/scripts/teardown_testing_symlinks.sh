#!/bin/bash

# This script removes symbolic links for SparkJobDefinition items.
# It unlinks the 'Libs' and 'Main' directories from the 'testing' directory.

# Loop through all SparkJobDefinition fabric items.
for dir in $(find fabric -type d -name "*.SparkJobDefinition"); do
    base_dir=$(basename "$dir") # Name without fabric prefix or any suffix
    
    # Unlink fabric Libs from testing Libs
    if [ -d "$PWD/$dir/Libs" ] && [ -d "$PWD/testing/$base_dir/Libs" ]; then
        for file in "$PWD/$dir/Libs"/*; do
            unlink "$PWD/testing/$base_dir/Libs/$(basename "$file")"
        done
    fi

    # Unlink fabric Libs from testing Main
    if [ -d "$PWD/$dir/Libs" ] && [ -d "$PWD/testing/$base_dir/Main" ]; then
        for file in "$PWD/$dir/Libs"/*; do
            unlink "$PWD/testing/$base_dir/Main/$(basename "$file")"
        done
    fi

    # Unlink fabric Main from testing Main
    if [ -d "$PWD/$dir/Main" ] && [ -d "$PWD/testing/$base_dir/Main" ]; then
        for file in "$PWD/$dir/Main"/*; do
            unlink "$PWD/testing/$base_dir/Main/$(basename "$file")"
        done
    fi
done
