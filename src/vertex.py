import sys

if(sys.argv[1]) == 'init':
    # print(sys.argv[3:])

    f = open(sys.argv[2] + '.txt', 'w')

    lst = sys.argv[3:]
    str = ' '.join(lst)
    f.write(str + '\n')

    f.close()

if(sys.argv[1]) == 'run':
    
    f = open(sys.argv[2] + '.txt', 'a')
    
    lst = sys.argv[3:]
    str = ' '.join(lst)
    f.write(str + '\n')

    f.close()

