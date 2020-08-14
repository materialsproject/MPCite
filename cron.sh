source /Users/michaelwu/Desktop/projects/MPCite/activate_conda
conda activate MPCite;
export CONFIG_FILE_PATH=/Users/michaelwu/Desktop/projects/MPCite/files/config_prod.json
mpcite --config_file_path $CONFIG_FILE_PATH;
conda deactivate