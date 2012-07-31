using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CodeAnalyzer.Graph
{
    public class Node<T>
    {
        private List<Node<T>> _fathers;
        private List<Node<T>> _childs;

        private T _data;

        public Node(T data)
        {
            _data = data;
            _fathers = new List<Node<T>>();
            _childs = new List<Node<T>>();
        }

        public List<Node<T>> Fathers
        {
            get { return _fathers; }
        }

        public List<Node<T>> Childs
        {
            get { return _childs; }
        }

        public T Data
        {
            get { return _data; }
        }

        public bool IsChildOf(Node<T> possibleFather)
        {
            return _fathers.Contains(possibleFather);
        }

        public bool IsFatherOf(Node<T> possibleChild)
        {
            return _childs.Contains(possibleChild);
        }

        public void AddFather(Node<T> father)
        {
            AddRelation(_fathers, father);
        }

        public void AddChild(Node<T> child)
        {
            AddRelation(_childs, child);
        }

        public void RemoveFather(Node<T> father)
        {
            RemoveRelation(_fathers, father);
        }

        public void RemoveChild(Node<T> child)
        {
            RemoveRelation(_childs, child);
        }

        private void AddRelation(List<Node<T>> nodes, Node<T> newRelation)
        {
            if (!nodes.Contains(newRelation))
                nodes.Add(newRelation);
            else
                throw new Exception("The relation already exists");
        }

        private void RemoveRelation(List<Node<T>> nodes, Node<T> relation)
        {
            if (nodes.Contains(relation))
                nodes.Remove(relation);
            else
                throw new Exception("The relation you are trying to delete doesn't exists");
        }
    }
}
