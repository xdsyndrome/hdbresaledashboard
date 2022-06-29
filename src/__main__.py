from Distance import HDBGeo
import click

@click.command()
@click.option('--download', default=False, help='Download data from gov.sg and OneMap and perform distance matching.')

def main(download):
    test = HDBGeo(download=download)  
    print(test.dataset_merged)

if __name__ == '__main__':
    main()
    