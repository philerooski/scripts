brewClean=true
condaClean=true
synapseCacheClean=false
downloadsClean=false

while true; do
    read -p "Clean Synapse Cache? " yn
    case $yn in
        [Yy]* ) synapseCacheClean=true; break;;
        [Nn]* ) break;;
        * ) echo "Please answer yes or no.";;
    esac
done

while true; do
    read -p "Clean Downloads? " yn
    case $yn in
        [Yy]* ) downloadsClean=true; break;;
        [Nn]* ) break;;
        * ) echo "Please answer yes or no.";;
    esac
done

if [ "$1" = "-v" ]; then
    while true; do
        read -p "Clean brew? " yn
        case $yn in
            [Yy]* ) break;;
            [Nn]* ) brewClean=false; break;;
            * ) echo "Please answer yes or no.";;
        esac
    done
    while true; do
        read -p "Clean conda? " yn
        case $yn in
            [Yy]* ) break;;
            [Nn]* ) condaClean=false; break;;
            * ) echo "Please answer yes or no.";;
        esac
    done
fi

if [ $brewClean = true ]; then
    brew cleanup
fi
if [ $condaClean = true ]; then
    conda clean --all -y
fi
if [ $synapseCacheClean = true ]; then
    rm -r ~/.synapseCache/*;
fi
if [ $downloadsClean = true ]; then
    rm -r ~/Downloads/*
fi
