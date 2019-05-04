def normalize(name):
  if name[-4:] == '.pho':
    name = name[:-4]
  return "{}.pho".format(name)
