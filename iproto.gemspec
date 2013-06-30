Gem::Specification.new do |s|
  s.specification_version = 2 if s.respond_to? :specification_version=
  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.rubygems_version = '1.3.5'

  s.name              = 'iproto'
  s.version           = '0.3.10'
  s.date              = '2013-06-30'
  s.rubyforge_project = 'iproto'

  s.summary     = "Mail.Ru simple network protocol"
  s.description = "Mail.Ru simple network protocol"

  s.authors  = ["Andrew Rudenko"]
  s.email    = 'ceo@prepor.ru'
  s.homepage = 'http://github.com/mailru/iproto-ruby'

  s.require_paths = %w[lib]

  s.rdoc_options = ["--charset=UTF-8"]
  s.extra_rdoc_files = %w[README.md LICENSE]

  # = MANIFEST =
  s.files = %w[
    LICENSE
    README.md
    Rakefile
    iproto.gemspec
    lib/iproto.rb
    lib/iproto/connection_api.rb
    lib/iproto/core-ext.rb
    lib/iproto/em.rb
    lib/iproto/tcp_socket.rb
  ]
  # = MANIFEST =

  ## Test files will be grabbed from the file list. Make sure the path glob
  ## matches what you actually use.
  s.test_files = s.files.select { |path| path =~ /^spec\/.*_spec\.rb/ }
  s.add_dependency 'bin_utils', ['~> 0.0.3']
end
