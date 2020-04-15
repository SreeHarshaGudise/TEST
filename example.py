gparty.join(tagntsg,[(gparty.id == tagntsg.id) & (gparty.agentid == tagntsg.agentid)],'inner').drop(tagntsg.agentid).show()
