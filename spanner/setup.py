import os
import re

from setuptools import setup

v = open(os.path.join(os.path.dirname(__file__),
                      'spannerdriver', 'sqlalchemy', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.md')


setup(name='spannerdriver.sqlalchemy',
      version=VERSION,
      description="Spanner for SQLAlchemy",
      long_description=open(readme).read(),
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: Implementation :: CPython',
          'Topic :: Database :: Front-Ends',
      ],
      keywords='SQLAlchemy Spanner',
      author='Jarek Potiuk',
      author_email='jarek.potiuk@polidea.com',
      license='MIT',
      packages=['spannerdriver.sqlalchemy'],
      include_package_data=True,
      tests_require=['nose >= 0.11'],
      test_suite="nose.collector",
      zip_safe=False,
      entry_points={
         'sqlalchemy.dialects': [
              'spanner = spannerdriver.sqlalchemy.googleapi:SpannerDialect_googleapi',
              ]
        }, install_requires=['sqlalchemy']
      )
