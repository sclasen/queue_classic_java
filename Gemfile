source :rubygems

gem "rake"

gemspec

group :test do
  gem "turn"
  gem "minitest"
  gem "rr"
end

platform :mri do
  gem "pg", "~> 0.13.2"
end

platform :jruby do
  gem "jdbc-postgres"
end