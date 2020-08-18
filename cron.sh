#source /home/mwu/MPCite/activate_conda
#source /home/mwu/.bashrc
source /home/mwu/anaconda3/etc/profile.d/conda.sh
conda activate mpcite
export CONFIG_FILE_PATH=/home/mwu/MPCite/files/config_prod.json;
mpcite --config_file_path $CONFIG_FILE_PATH;
conda deactivate;
