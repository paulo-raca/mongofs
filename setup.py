from setuptools import setup

setup(
  name = 'mongofs',
  packages = ['mongofs'], # this must be the same as the name above
  version = '0.1.0.1',
  description = 'Access Mongo documents in a FUSE filesystem',
  author = 'Paulo Costa',
  author_email = 'me@paulo.costa.nom.br',
  url = 'https://github.com/paulo-raca/mongofs',
  download_url = 'https://github.com/paulo-raca/mongofs/0.1',
  keywords = ['fuse', 'mongo'],
  entry_points = {
      'console_scripts': ['mount.mongofs=mongofs.__main__:main'],
  },
  install_requires = [
    "RouteFS",
    "notify2",
    "pymongo",
    "expiringdict",
    "procfs"
  ]
)