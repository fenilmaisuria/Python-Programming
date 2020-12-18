board = {1:1,2:2,3:3,4:4,5:5,6:6,7:7,8:8,9:9}

def boarddesign(b):
    print(b[1],'|',b[2],'|',b[3])
    print("--+---+--")
    print(b[4],'|',b[5],'|',b[6])
    print("--+---+--")
    print(b[7],'|',b[8],'|',b[9])
    
boarddesign(board)

def checkmatch(sign):
    if board[1] == sign and board[2] == sign and board[3] == sign:
        print("user {} is winner".format(sign))
        return True
    elif board[4] == sign and board[5] == sign and board[6] == sign:
        print("user {} is winner".format(sign))
        return True
    elif board[7] == sign and board[8] == sign and board[9] == sign:
        print("user {} is winner".format(sign))
        return True
    elif board[1] == sign and board[4] == sign and board[7] == sign:
        print("user {} is winner".format(sign))
        return True
    elif board[2] == sign and board[5] == sign and board[8] == sign:
        print("user {} is winner".format(sign))
        return True
    elif board[3] == sign and board[6] == sign and board[9] == sign:
        print("user {} is winner".format(sign))
        return True
    elif board[1] == sign and board[5] == sign and board[9] == sign:
        print("user {} is winner".format(sign))
        return True
    elif board[3] == sign and board[5] == sign and board[7] == sign:
        print("user {} is winner".format(sign))
        return True

def movecheck(sign):
    move =int(input())
    if(move == board[move]):
        print("Move : ",move," board[move] : ",board[move])
        board[move] = sign
        boarddesign(board)
    else:
        print("Already taken")
        move =int(input())
        print("else Move : ",move," board[move] : ",board[move])
        board[move] = sign
        boarddesign(board)
        
    if i > 5:
        print("condition check")
        checkmatch(sign)
        

for i in board:
    if (i % 2) == 0:
        usersign = 'X'
        print("condition i ",i,"board value : ",board[i])
        if(i == board[i]):
            movecheck(usersign)
            v = checkmatch(usersign)
            if  v == True :
                break
        else:
            print("Already input is taken")
            break
    else:
        usersign = 'O'
        print(board[i])
        if(i == board[i]):
            movecheck(usersign)
            v = checkmatch(usersign)
            if  v == True :
                break
        else:
            print("Already input is taken")
            break