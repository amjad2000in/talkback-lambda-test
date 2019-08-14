from setuptools import setup

exec(open('talkback/version.py').read())

setup(name='talkback',
      version=__version__,
      description='Talkback project core module',
      url='https://github.com/SumnerLab/talback-lambda/tbproject',
      license='Proprietary',
      author='Paul Little',
      author_email='plittle@EvolutionHosting.com',
      packages=['talkback'],
      install_requires=['boto3', 'requests'],
      zip_safe=False)
