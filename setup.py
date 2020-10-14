from setuptools import setup, find_packages

setup(name='salem',
      version='0.0.1',
      description='Incipia Python AppsFlyer API client.',
      url='https://github.com/xyla-io/salem',
      author='Gregory Klein',
      author_email='gklei89@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
        'pandas',
        'requests',
        'furl',
        'boto3',
      ],
      zip_safe=False)
